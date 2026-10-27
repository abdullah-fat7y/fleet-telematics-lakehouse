"""Batch extractor: source database -> Lakehouse Files/landing_v2, one hour slice at a time.

    python extract.py --date 2026-11-02                 # initial load: all 24 hours of a day
    python extract.py --incremental                     # every hour with data that was never extracted
    python extract.py --rerun 2026-11-03T06             # extract an hour again (idempotency check)
    python extract.py --backfill 2026-11-02T10          # reprocess an old hour
    python extract.py --incremental --fail-after 1      # crash in the middle of the 2nd hour (retry demo)

Each hour of telemetry_history becomes one gzip JSON-lines file
    Files/landing_v2/event_date=YYYY-MM-DD/event_hour=HH/part-<extract_id>.jsonl.gz
with one envelope per source row: {db_id, raw, is_valid, reject_reason, run_id, db_received_at,
slice_hour, source: "db_extract", extract_id}. Every hour is recorded in extract_log (started -> done);
an hour that is not 'done' is picked up again by the next --incremental run, so a crash loses nothing.
Files are never overwritten (new extract_id each time); the Fabric loader (notebook 11) inserts rows by
db_id only once, so re-runs and backfills cannot create duplicates in the cloud.
"""
import argparse
import gzip
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
GENERATOR = os.path.join(HERE, "..", "..", "generator")
sys.path.insert(0, GENERATOR)
from fleet.common import load_config  # noqa: E402
from send_stream import read_dotenv  # noqa: E402
from upload_to_onelake import connect, upload  # noqa: E402

DSN = os.environ.get("SOURCE_DB", "postgresql://fleet:fleet_local@localhost:55432/fleet")
LOCAL_DIR = os.path.join(HERE, "output", "landing_v2")
CRASH_AT_ROW = int(os.environ.get("CRASH_AT_ROW", "50000"))  # where --fail-after "kills" the hour


def parse_hour(text):
    return datetime.strptime(text, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)


def hours_to_extract(conn, args):
    if args.date:
        day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        rows = conn.execute("SELECT DISTINCT slice_hour FROM telemetry_history WHERE slice_hour >= %s "
                            "AND slice_hour < %s + interval '1 day' ORDER BY 1", (day, day)).fetchall()
        return "initial", [r[0] for r in rows]
    if args.incremental:
        rows = conn.execute("""SELECT DISTINCT h.slice_hour FROM telemetry_history h
                               WHERE NOT EXISTS (SELECT 1 FROM extract_log l
                                                 WHERE l.slice_hour = h.slice_hour AND l.status = 'done')
                               ORDER BY 1""").fetchall()
        return "incremental", [r[0] for r in rows]
    mode, values = ("rerun", args.rerun) if args.rerun else ("backfill", args.backfill)
    return mode, [parse_hour(v) for v in values]


def extract_hour(conn, fs, root, extract_id, hour, mode, delays, crash_midway=False):
    conn.execute("INSERT INTO extract_log (extract_id, slice_hour, mode, status) VALUES (%s, %s, %s, 'started')",
                 (extract_id, hour, mode))
    conn.commit()
    rel = f"event_date={hour:%Y-%m-%d}/event_hour={hour:%H}/part-{extract_id}.jsonl.gz"
    local = os.path.join(LOCAL_DIR, rel)
    os.makedirs(os.path.dirname(local), exist_ok=True)
    rows = rejected = 0
    with conn.cursor(name=f"slice_{hour:%Y%m%d%H}") as cur, gzip.open(local + ".tmp", "wt") as f:
        cur.itersize = 20000
        cur.execute("SELECT id, raw, is_valid, reject_reason, run_id, received_at FROM telemetry_history "
                    "WHERE slice_hour = %s ORDER BY id", (hour,))
        for db_id, raw, is_valid, reason, run_id, received_at in cur:
            f.write(json.dumps({"db_id": db_id, "raw": raw, "is_valid": is_valid, "reject_reason": reason,
                                "run_id": run_id, "db_received_at": received_at.isoformat(),
                                "slice_hour": hour.isoformat(), "source": "db_extract",
                                "extract_id": extract_id}, separators=(",", ":")) + "\n")
            rows += 1
            rejected += not is_valid
            if crash_midway and rows == CRASH_AT_ROW:
                print(f"  simulated crash while extracting {hour:%Y-%m-%d %H}:00 (file half written, not uploaded)",
                      flush=True)  # os._exit skips the normal flush of buffered output
                os._exit(1)  # like a kill -9: no cleanup, extract_log stays 'started'
    os.replace(local + ".tmp", local)
    if fs is not None and not upload(fs, local, f"{root}/landing_v2/{rel}", delays):
        raise RuntimeError(f"upload failed for {rel}; hour stays 'started' and is retried next run")
    conn.execute("UPDATE extract_log SET status = 'done', rows = %s, rejected = %s, file = %s, "
                 "finished_at = clock_timestamp() WHERE extract_id = %s AND slice_hour = %s",
                 (rows, rejected, rel, extract_id, hour))
    conn.commit()
    return rows, rejected


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="initial load: every hour of this day (YYYY-MM-DD)")
    g.add_argument("--incremental", action="store_true", help="every hour with data that was never extracted")
    g.add_argument("--rerun", nargs="+", metavar="YYYY-MM-DDTHH", help="extract these hours again")
    g.add_argument("--backfill", nargs="+", metavar="YYYY-MM-DDTHH", help="reprocess these old hours")
    ap.add_argument("--fail-after", type=int, help="simulate a crash in the middle of hour N+1")
    ap.add_argument("--no-upload", action="store_true", help="write the files locally only (testing)")
    args = ap.parse_args()

    cfg = load_config(os.path.join(GENERATOR, "config.yaml"))
    read_dotenv(os.path.join(GENERATOR, ".env"))
    fs, root = (None, None) if args.no_upload else connect(cfg)
    # x<UTC time>-<random>: unique even when two runs start in the same second
    extract_id = datetime.now(timezone.utc).strftime("x%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:6]
    with psycopg.connect(DSN) as conn:
        mode, hours = hours_to_extract(conn, args)
        print(f"{mode} extract {extract_id}: {len(hours)} hour slice(s)")
        started, total, total_rej = time.time(), 0, 0
        for i, hour in enumerate(hours):
            t0 = time.time()
            rows, rej = extract_hour(conn, fs, root, extract_id, hour, mode, cfg["streaming"]["retry_delays_s"],
                                     crash_midway=args.fail_after is not None and i == args.fail_after)
            total, total_rej = total + rows, total_rej + rej
            print(f"  {hour:%Y-%m-%d %H}:00  {rows:>8,} rows ({rej} rejected)  {time.time() - t0:5.1f} s", flush=True)
    duration = time.time() - started
    summary = {"extract_id": extract_id, "mode": mode, "hours": len(hours), "rows": total, "rejected": total_rej,
               "duration_s": round(duration, 1), "rows_per_s": round(total / duration, 1) if duration else None}
    os.makedirs(os.path.join(HERE, "output"), exist_ok=True)
    with open(os.path.join(HERE, "output", "extract_runs.jsonl"), "a") as f:
        f.write(json.dumps(summary) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
