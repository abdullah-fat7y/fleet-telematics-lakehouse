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


class Run:
    """Shared counters, outputs and logs for one full run."""

    def __init__(self, cfg, base_dir, n_vehicles, hours, workers, fail_first, dry_run):
        self.cfg, self.base_dir = cfg, base_dir
        self.run_id = datetime.now(timezone.utc).strftime("full-%Y%m%dT%H%M%SZ")
        self.n_vehicles, self.hours, self.workers, self.dry_run = n_vehicles, hours, workers, dry_run
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
            "run_id": self.run_id, "measured": True, "dry_run": self.dry_run,
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
            w.writerow([self.run_id, "stream", datetime.fromtimestamp(self.started, timezone.utc).isoformat(),
                        perf["duration_s"], self.generated, self.sent, self.dlq, self.retries,
                        perf["events_per_s"], self.dry_run])
        return perf


class Worker:
    def __init__(self, run, ref, vehicles, end_time, client, partitions):
        self.run, self.cfg, self.ref = run, run.cfg, ref
        self.vehicles, self.end_time = vehicles, end_time
        self.client, self.partitions = client, partitions
        self.buffers = {}

    def _partition(self, vehicle_id):
        return self.partitions[zlib.crc32(vehicle_id.encode()) % len(self.partitions)] if self.partitions else None

    async def _send(self, partition, envelopes):
        delays = self.cfg["streaming"]["retry_delays_s"]
        for attempt in range(len(delays) + 1):
            try:
                if self.run.fail_remaining > 0:
                    self.run.fail_remaining -= 1
                    raise ConnectionError("simulated transient failure")
                if self.client is not None:
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
                    return
                self.run.retries += 1
                await asyncio.sleep(delays[attempt])

    async def _add(self, vehicle_id, raw):
        partition = self._partition(vehicle_id)
        buf = self.buffers.setdefault(partition, [])
        buf.append({"raw": raw, "source": "stream", "sent_at_utc": now_utc(), "run_id": self.run.run_id})
        if len(buf) >= self.cfg["streaming"]["batch_max_events"]:
            self.buffers[partition] = []
            await self._send(partition, buf)

    async def run_all(self):
        streams = [vehicle_stream(self.cfg, self.ref, v, self.end_time) for v in self.vehicles]
        bad_rngs = {v["vehicle_id"]: rng_for(self.cfg["seed"], "bad", v["vehicle_id"]) for v in self.vehicles}
        delay = timedelta(hours=self.cfg["bad_data"]["late_event_delay_hours"])
        late = []  # (due_time, vehicle_id, raw) held back and released later
        n = 0
        for event_time, vehicle_id, event, tag in heapq.merge(*streams, key=lambda x: (x[0], x[1])):
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
                    fail_first=0, dry_run=False):
    ref = load_reference(os.path.join(base_dir, cfg["output"]["reference_dir"]))
    vehicles = sorted(ref["vehicles"].values(), key=lambda v: v["vehicle_id"])[:max_vehicles]
    hours = hours or cfg["simulation"]["hours"]
    workers = workers or cfg["streaming"]["workers"]
    end_time = parse_utc(cfg["simulation"]["start_time_utc"]) + timedelta(hours=hours)
    run = Run(cfg, base_dir, len(vehicles), hours, workers, fail_first, dry_run)
    print(f"run {run.run_id}: {len(vehicles)} vehicles, {hours} simulated h, {workers} workers"
          f"{' (dry run)' if dry_run else ''}")

    clients, partitions = [], None
    if not dry_run:
        from azure.eventhub.aio import EventHubProducerClient
        clients = [EventHubProducerClient.from_connection_string(connection_string, retry_total=0)
                   for _ in range(workers)]
        partitions = await clients[0].get_partition_ids()
        print(f"Eventstream partitions: {len(partitions)}")

    pool = [Worker(run, ref, vehicles[i::workers], end_time, clients[i] if clients else None, partitions)
            for i in range(workers)]
    logger = asyncio.create_task(run.interval_logger())
    try:
        await asyncio.gather(*(w.run_all() for w in pool))
    finally:
        logger.cancel()
        for c in clients:
            await c.close()
    return run.finish()
