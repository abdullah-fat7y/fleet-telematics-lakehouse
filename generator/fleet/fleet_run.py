"""Full run: every vehicle is generated and streamed by a pool of async workers.

- Vehicles are split round-robin across workers; a vehicle always stays on the same worker,
  and each worker emits its vehicles' readings in event_time order.
- Every vehicle always goes to the same Eventstream partition (crc32 of vehicle_id),
  so its readings stay in order downstream while batches can mix vehicles.
- Anomalies and bad data use the rates in config.yaml (no forcing, unlike the sample).
- Same envelope, retry/backoff and DLQ rules as send_stream.py.
"""
import asyncio
import csv
import heapq
import itertools
import json
import os
import platform
import time
import zlib
from datetime import datetime, timedelta, timezone

from .bad_data import LATE, corrupt, pick_bad_type, to_raw
from .common import fmt_utc, parse_utc, rng_for, write_csv
from .reference import load_reference
from .simulate import plan_episodes, simulate_trip
from .stream_sender import now_utc


def vehicle_stream(cfg, ref, vehicle, end_time):
    """Yields (event_time, vehicle_id, event, truth_tag) for all of a vehicle's trips in the window."""
    rng = rng_for(cfg["seed"], "vehicle", vehicle["vehicle_id"])
    dt = cfg["simulation"]["reading_interval_s"]
    odometer, last_time, counter = float(vehicle["start_odometer_km"]), None, {}
    for trip in ref["trips"][vehicle["vehicle_id"]]:
        trip = dict(trip)
        start = parse_utc(trip["planned_start_utc"])
        if last_time is not None and start <= last_time:
            # A slow first leg (idling, speed noise) must never overlap the next one.
            start = last_time + timedelta(seconds=dt)
            trip["planned_start_utc"] = fmt_utc(start)
        if start >= end_time:
            break
        episodes = plan_episodes(cfg, rng, vehicle, trip, counter)
        path = ref["paths"][trip["corridor_id"]]
        for event, tag in simulate_trip(cfg, rng, vehicle, trip, path, odometer, episodes, end_time=end_time):
            odometer, last_time = event["odometer_km"], parse_utc(event["event_time"])
            yield event["event_time"], event["vehicle_id"], event, tag


class Pacer:
    """Token bucket shared by all workers: holds the overall send rate at `rate` events/s,
    so a run behaves like a steady live feed instead of one burst."""

    def __init__(self, rate):
        self.rate, self.tokens, self.last = rate, 0.0, time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self, n):
        async with self.lock:
            while True:
                now = time.monotonic()
                self.tokens = min(max(self.rate, n), self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                await asyncio.sleep((n - self.tokens) / self.rate)


class Run:
    """Shared counters, outputs and logs for one full run."""

    def __init__(self, cfg, base_dir, n_vehicles, hours, workers, fail_first, dry_run, mode="stream",
                 channel=None, rate=None, minutes=None):
        self.cfg, self.base_dir = cfg, base_dir
        # "stream" (Eventstream), "batch" (files for Files/landing) or "api" (ingestion API, channel live/history)
        self.mode, self.channel = mode, channel
        prefix = {"batch": "batch", "api": f"api-{channel}"}.get(mode, "full")
        self.run_id = datetime.now(timezone.utc).strftime(f"{prefix}-%Y%m%dT%H%M%SZ")
        self.pacer = Pacer(rate) if rate else None
        self.rate, self.minutes = rate, minutes
        self.deadline = None
        self.n_vehicles, self.hours, self.workers, self.dry_run = n_vehicles, hours, workers, dry_run
        self.landing_dir = os.path.join(base_dir, cfg["output"]["landing_dir"], self.run_id)
        self.files_written = 0
        self.fail_remaining = fail_first
        self.generated = self.sent = self.dlq = self.retries = 0
        self.truth, self.manifest = [], []
        self.active_in_interval = set()
        self.started = time.time()
        out = cfg["output"]
        self.run_dir = os.path.join(base_dir, out["runs_dir"], self.run_id)
        self.dlq_path = os.path.join(base_dir, out["send_dlq_file"])
        self.interval_path = os.path.join(base_dir, out["interval_log_file"])
        self.send_log_path = os.path.join(base_dir, out["send_log_file"])

    def start_clock(self):
        """Called after the fast-forward: duration, rate and the wall-clock limit count from here."""
        self.started = time.time()
        self.deadline = self.started + self.minutes * 60 if self.minutes else None
        if self.pacer:
            self.pacer.last = time.monotonic()

    def dead_letter(self, envelopes, reason, retries):
        os.makedirs(os.path.dirname(self.dlq_path), exist_ok=True)
        with open(self.dlq_path, "a") as f:
            for env in envelopes:
                f.write(json.dumps({"raw": env["raw"], "source": env["source"], "reason": reason,
                                    "failed_at_utc": now_utc(), "retries": retries, "run_id": self.run_id}) + "\n")
        self.dlq += len(envelopes)

    async def interval_logger(self):
        """One row per interval: received (generated), written (sent), rejected (DLQ), duration."""
        every = self.cfg["streaming"]["log_interval_s"]
        last = (time.time(), 0, 0, 0)
        os.makedirs(os.path.dirname(self.interval_path), exist_ok=True)
        new = not os.path.exists(self.interval_path)
        with open(self.interval_path, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["run_id", "interval_start_utc", "duration_s", "events_received", "events_written",
                            "events_rejected", "active_vehicles", "events_per_s"])
            while True:
                await asyncio.sleep(every)
                now = time.time()
                written = self.sent - last[2]
                w.writerow([self.run_id, datetime.fromtimestamp(last[0], timezone.utc).isoformat(),
                            round(now - last[0], 2), self.generated - last[1], written, self.dlq - last[3],
                            len(self.active_in_interval), round(written / (now - last[0]), 1)])
                f.flush()
                print(f"  {self.generated:>9,} generated  {self.sent:>9,} sent  {self.dlq} DLQ  "
                      f"{written / (now - last[0]):,.0f} ev/s  {len(self.active_in_interval)} vehicles")
                self.active_in_interval = set()
                last = (now, self.generated, self.sent, self.dlq)

    def finish(self):
        duration = time.time() - self.started
        write_csv(os.path.join(self.run_dir, "telemetry_truth.csv"), self.truth,
                  ["event_id", "anomaly_type", "anomaly_group_id"])
        write_csv(os.path.join(self.run_dir, "bad_data_manifest.csv"), self.manifest,
                  ["event_id", "bad_type", "detail"])
        perf = {
            "run_id": self.run_id, "mode": self.mode, "channel": self.channel, "measured": True,
            "dry_run": self.dry_run, "target_rate_eps": self.rate, "wall_clock_limit_min": self.minutes,
            "files_written": self.files_written if self.mode == "batch" else None,
            "vehicles": self.n_vehicles, "simulated_hours": self.hours, "workers": self.workers,
            "events_generated": self.generated, "events_sent": self.sent, "events_dlq": self.dlq,
            "retries": self.retries, "duration_s": round(duration, 1),
            "events_per_s": round(self.sent / duration, 1) if duration else None,
            "anomaly_episodes": {k: len({t["anomaly_group_id"] for t in self.truth if t["anomaly_type"] == k})
                                 for k in sorted({t["anomaly_type"] for t in self.truth})},
            "bad_data_events": {k: sum(m["bad_type"] == k for m in self.manifest)
                                for k in sorted({m["bad_type"] for m in self.manifest})},
            "machine": {"platform": platform.platform(), "processor": platform.processor() or platform.machine(),
                        "cpu_count": os.cpu_count(), "python": platform.python_version()},
        }
        with open(os.path.join(self.run_dir, "perf.json"), "w") as f:
            json.dump(perf, f, indent=2)
        new = not os.path.exists(self.send_log_path)
        os.makedirs(os.path.dirname(self.send_log_path), exist_ok=True)
        with open(self.send_log_path, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["run_id", "source", "started_utc", "duration_s", "events_read", "events_sent",
                            "events_dlq", "retries", "events_per_s", "dry_run"])
            w.writerow([self.run_id, self.channel or self.mode, datetime.fromtimestamp(self.started, timezone.utc).isoformat(),
                        perf["duration_s"], self.generated, self.sent, self.dlq, self.retries,
                        perf["events_per_s"], self.dry_run])
        return perf


class Worker:
    def __init__(self, run, ref, vehicles, start_time, end_time, client, partitions, index=0):
        self.run, self.cfg, self.ref = run, run.cfg, ref
        self.vehicles, self.end_time = vehicles, end_time
        self.window_start = fmt_utc(start_time)  # readings before this are simulated (for state) but not sent
        self.client, self.partitions = client, partitions
        self.index, self.files = index, 0
        self.buffers = {}
        self.flush_at = {"batch": self.cfg["batch"]["file_max_events"],
                         "api": self.cfg["api"]["batch_events_history" if run.channel == "history" else "batch_events"]
                         }.get(run.mode, self.cfg["streaming"]["batch_max_events"])

    def _write_file(self, envelopes):
        """Batch mode: one JSON-lines file of raw events, written to .tmp first so an uploader
        never picks up a half-written file. The file name carries the write time (= sent_at)."""
        os.makedirs(self.run.landing_dir, exist_ok=True)
        self.files += 1
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        path = os.path.join(self.run.landing_dir, f"{self.run.run_id}_w{self.index:02d}_{self.files:05d}_{stamp}.jsonl")
        with open(path + ".tmp", "w") as f:
            f.write("\n".join(env["raw"] for env in envelopes) + "\n")
        os.replace(path + ".tmp", path)
        self.run.files_written += 1

    def _partition(self, vehicle_id):
        return self.partitions[zlib.crc32(vehicle_id.encode()) % len(self.partitions)] if self.partitions else None

    async def _send(self, partition, envelopes):
        delays = self.cfg["streaming"]["retry_delays_s"]
        if self.run.pacer:
            await self.run.pacer.acquire(len(envelopes))
        for attempt in range(len(delays) + 1):
            try:
                if self.run.fail_remaining > 0:
                    self.run.fail_remaining -= 1
                    raise ConnectionError("simulated transient failure")
                if self.run.mode == "batch" and not self.run.dry_run:
                    self._write_file(envelopes)
                elif self.run.mode == "api" and self.client is not None:
                    # One POST per batch: the raw lines exactly as generated (malformed ones included).
                    resp = await self.client.post(
                        "/v1/telemetry", params={"channel": self.run.channel},
                        content="\n".join(env["raw"] for env in envelopes).encode(),
                        headers={"Content-Type": "application/x-ndjson", "X-Run-Id": self.run.run_id,
                                 "X-Sent-At": now_utc()})
                    if resp.status_code >= 300:
                        raise ConnectionError(f"API returned {resp.status_code}: {resp.text[:200]}")
                elif self.client is not None:
                    from azure.eventhub import EventData
                    batch = await self.client.create_batch(partition_id=partition)
                    for env in envelopes:
                        batch.add(EventData(json.dumps(env, separators=(",", ":"))))
                    await self.client.send_batch(batch)
                self.run.sent += len(envelopes)
                return
            except Exception as exc:  # treated as temporary until the retries run out
                if attempt == len(delays):
                    self.run.dead_letter(envelopes, f"{type(exc).__name__}: {exc}", attempt)
                    print(f"  gave up after {attempt} retries -> {len(envelopes)} events to DLQ")
                    return
                self.run.retries += 1
                print(f"  send failed ({exc}); retry {attempt + 1}/{len(delays)} in {delays[attempt]}s")
                await asyncio.sleep(delays[attempt])

    async def _add(self, vehicle_id, raw):
        partition = self._partition(vehicle_id)
        buf = self.buffers.setdefault(partition, [])
        buf.append({"raw": raw, "source": self.run.channel or self.run.mode, "sent_at_utc": now_utc(), "run_id": self.run.run_id})
        if len(buf) >= self.flush_at:
            self.buffers[partition] = []
            await self._send(partition, buf)

    def prepare(self):
        """Simulate everything before the window (vehicle state only, nothing is sent) and stop at the
        first reading inside it, so the wall-clock limit and the pacing only cover the real run."""
        streams = [vehicle_stream(self.cfg, self.ref, v, self.end_time) for v in self.vehicles]
        self.merged = heapq.merge(*streams, key=lambda x: (x[0], x[1]))
        self.first = next((item for item in self.merged if item[0] >= self.window_start), None)

    async def run_all(self):
        bad_rngs = {v["vehicle_id"]: rng_for(self.cfg["seed"], "bad", v["vehicle_id"]) for v in self.vehicles}
        delay = timedelta(hours=self.cfg["bad_data"]["late_event_delay_hours"])
        late = []  # (due_time, vehicle_id, raw) held back and released later
        n = 0
        for event_time, vehicle_id, event, tag in itertools.chain([self.first] if self.first else [], self.merged):
            if self.run.deadline and time.time() >= self.run.deadline:
                break  # wall-clock limit reached: stop generating, then flush what is buffered
            self.run.generated += 1
            self.run.active_in_interval.add(vehicle_id)
            while late and late[0][0] <= event_time:
                _, vid, raw = heapq.heappop(late)
                await self._add(vid, raw)
            if tag:
                self.run.truth.append({"event_id": event["event_id"], "anomaly_type": tag[0],
                                       "anomaly_group_id": tag[1]})
                raws = [to_raw(event)]
            else:
                rng = bad_rngs[vehicle_id]
                kind = pick_bad_type(self.cfg, rng)
                if kind is None:
                    raws = [to_raw(event)]
                else:
                    raws, detail = corrupt(rng, event, kind)
                    self.run.manifest.append({"event_id": event["event_id"], "bad_type": kind, "detail": detail})
                    if kind == LATE:
                        due = fmt_utc(parse_utc(event_time) + delay)
                        heapq.heappush(late, (due, vehicle_id, raws[0]))
                        continue
            for raw in raws:
                await self._add(vehicle_id, raw)
            n += 1
            if n % 500 == 0:
                await asyncio.sleep(0)  # let the other workers and the logger run
        for _, vid, raw in sorted(late):  # still-late events arrive at the end of the run
            await self._add(vid, raw)
        for partition, buf in self.buffers.items():
            if buf:
                await self._send(partition, buf)


async def run_fleet(cfg, base_dir, connection_string, hours=None, max_vehicles=None, workers=None,
                    fail_first=0, dry_run=False, from_hours=0.0, mode="stream", channel=None, rate=None,
                    minutes=None):
    ref = load_reference(os.path.join(base_dir, cfg["output"]["reference_dir"]))
    vehicles = sorted(ref["vehicles"].values(), key=lambda v: v["vehicle_id"])[:max_vehicles]
    hours = hours or cfg["simulation"]["hours"]
    workers = workers or cfg["streaming"]["workers"]
    sim_start = parse_utc(cfg["simulation"]["start_time_utc"])
    start_time, end_time = sim_start + timedelta(hours=from_hours), sim_start + timedelta(hours=hours)
    run = Run(cfg, base_dir, len(vehicles), hours, workers, fail_first, dry_run, mode, channel, rate, minutes)
    print(f"{mode} run {run.run_id}: {len(vehicles)} vehicles, simulated h {from_hours}-{hours}, {workers} workers"
          f"{f', paced at {rate} events/s' if rate else ''}{f', stops after {minutes} min' if minutes else ''}"
          f"{' (dry run)' if dry_run else ''}")
    if from_hours >= 24:
        print(f"  fast-forwarding {from_hours:.0f} simulated hours first so every vehicle starts in its real state")

    clients, partitions = [], None
    if mode == "api" and not dry_run:
        import httpx
        clients = [httpx.AsyncClient(base_url=cfg["api"]["url"], timeout=60) for _ in range(workers)]
    if mode == "stream" and not dry_run:
        from azure.eventhub.aio import EventHubProducerClient
        clients = [EventHubProducerClient.from_connection_string(connection_string, retry_total=0)
                   for _ in range(workers)]
        partitions = await clients[0].get_partition_ids()
        print(f"Eventstream partitions: {len(partitions)}")

    pool = [Worker(run, ref, vehicles[i::workers], start_time, end_time, clients[i] if clients else None, partitions, i)
            for i in range(workers)]
    for w in pool:
        w.prepare()
    run.start_clock()
    logger = asyncio.create_task(run.interval_logger())
    try:
        await asyncio.gather(*(w.run_all() for w in pool))
    finally:
        logger.cancel()
        for c in clients:
            await (c.aclose() if mode == "api" else c.close())
    perf = run.finish()
    if mode == "batch" and not dry_run:
        print(f"wrote {run.files_written} files to {run.landing_dir}")
    return perf
