"""Fleet ingestion API: the entry point of the pipeline.

Vehicles (the generator) POST raw telemetry lines; the API validates every line and stores it in the
source database, valid or not, so nothing is lost:

    POST /v1/telemetry?channel=live|history   body: one raw JSON event per line (application/x-ndjson)
    GET  /v1/stats                             row counts per table (for reconciliation)
    GET  /health

channel=live    -> telemetry_live    (Debezium streams it into Kafka, then on to the cloud)
channel=history -> telemetry_history (the batch extractor exports it hour by hour)
Validation uses the same rules as the Fabric notebook 04, so both ends agree on what is invalid.
"""
import json
import os
import time
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Request

FIELDS = ["event_id", "vehicle_id", "trip_id", "event_time", "latitude", "longitude", "speed_kmh",
          "rpm", "engine_temp_c", "fuel_rate_lph", "odometer_km", "cargo_temp_c"]
REQUIRED = [f for f in FIELDS if f != "cargo_temp_c"]  # cargo_temp_c is empty for dry vans by design
NUMERIC = ["latitude", "longitude", "speed_kmh", "rpm", "engine_temp_c", "fuel_rate_lph", "odometer_km", "cargo_temp_c"]
TABLES = {"live": "telemetry_live", "history": "telemetry_history"}
COLUMNS = ["raw", "vehicle_id", "event_time", "slice_hour", "is_valid", "reject_reason", "run_id", "sent_at"]

app = FastAPI(title="Fleet ingestion API")


@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=2, max_size=20)


def parse_time(value):
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def is_number(value):
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def validate(line):
    """Returns (vehicle_id, event_time, reasons). Reasons empty = valid."""
    try:
        event = json.loads(line)
    except ValueError:
        return None, None, ["malformed_json: not valid JSON"]
    if not isinstance(event, dict):
        return None, None, ["malformed_json: not a JSON object"]
    reasons = []
    for f in REQUIRED:
        if f not in event:
            reasons.append(f"missing_field: {f}")
        elif event[f] is None:
            reasons.append(f"null_required: {f}")
    for f in NUMERIC:
        if event.get(f) is not None and not is_number(event[f]):
            reasons.append(f"wrong_type: {f} is not a number")
    event_time = parse_time(event.get("event_time"))
    if event.get("event_time") is not None and event_time is None:
        reasons.append("wrong_type: event_time is not ISO 8601 UTC")
    vehicle_id = event.get("vehicle_id") if isinstance(event.get("vehicle_id"), str) else None
    return vehicle_id, event_time, reasons


@app.post("/v1/telemetry")
async def ingest(request: Request, channel: str = Query("live", pattern="^(live|history)$")):
    started = time.perf_counter()
    body = (await request.body()).decode("utf-8", errors="replace")
    lines = [line for line in body.split("\n") if line.strip()]
    if not lines:
        raise HTTPException(400, "empty body: send one JSON event per line")
    run_id, sent_at = request.headers.get("X-Run-Id"), request.headers.get("X-Sent-At")

    checked = [validate(line) for line in lines]
    # Unreadable lines have no event time; they are filed under the hour of the nearest readable
    # line in the same request (requests are in event-time order), else the time of arrival.
    known = [t for _, t, _ in checked if t is not None]
    fallback = known[0] if known else datetime.now(timezone.utc)
    rows, last_time, valid = [], fallback, 0
    for line, (vehicle_id, event_time, reasons) in zip(lines, checked):
        last_time = event_time or last_time
        valid += not reasons
        rows.append((line, vehicle_id, event_time, last_time.replace(minute=0, second=0, microsecond=0),
                     not reasons, "; ".join(reasons) or None, run_id, sent_at))

    async with app.state.pool.acquire() as conn:
        async with conn.transaction():  # the lines and their log row are stored together or not at all
            await conn.copy_records_to_table(TABLES[channel], records=rows, columns=COLUMNS)
            duration_ms = (time.perf_counter() - started) * 1000
            await conn.execute("INSERT INTO api_ingest_log (channel, run_id, lines, valid, rejected, duration_ms) "
                               "VALUES ($1, $2, $3, $4, $5, $6)", channel, run_id, len(lines), valid,
                               len(lines) - valid, duration_ms)
    return {"channel": channel, "received": len(lines), "valid": valid, "rejected": len(lines) - valid}


@app.get("/v1/stats")
async def stats():
    async with app.state.pool.acquire() as conn:
        out = {}
        for channel, table in TABLES.items():
            row = await conn.fetchrow(f"SELECT count(*) AS rows, count(*) FILTER (WHERE NOT is_valid) AS rejected, "
                                      f"min(event_time) AS first_event, max(event_time) AS last_event FROM {table}")
            out[channel] = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(row).items()}
        out["requests"] = await conn.fetchval("SELECT count(*) FROM api_ingest_log")
        return out


@app.get("/health")
async def health():
    async with app.state.pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok"}
