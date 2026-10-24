"""Sends raw event lines to the Fabric Eventstream custom endpoint.

Every event travels inside a small envelope so that even malformed JSON reaches
bronze untouched (the Eventstream Lakehouse writer would otherwise drop it):

    {"raw": "<event text exactly as generated>", "source": "stream", "sent_at_utc": "...", "run_id": "..."}

Temporary failures are retried after 1, 2, 4, 8 seconds (from config); after the
last retry the events go to a local dead-letter file with the reason, so nothing
disappears silently.
"""
import csv
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone

VEHICLE_RE = re.compile(r'"vehicle_id":"(VEH-\d+)"')


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class StreamSender:
    def __init__(self, cfg, base_dir, connection_string=None, dry_run=False, fail_first=0):
        self.cfg = cfg["streaming"]
        self.dlq_path = os.path.join(base_dir, cfg["output"]["send_dlq_file"])
        self.log_path = os.path.join(base_dir, cfg["output"]["send_log_file"])
        self.fail_remaining = fail_first  # simulate outages to demonstrate retry + DLQ
        self.run_id = str(uuid.uuid4())
        self.client = None
        if not dry_run:
            from azure.eventhub import EventHubProducerClient
            # SDK retries are turned off so our own 1-2-4-8 s backoff is the one that runs (and is logged).
            self.client = EventHubProducerClient.from_connection_string(connection_string, retry_total=0)
        self.stats = {"events_read": 0, "events_sent": 0, "events_dlq": 0, "retries": 0}

    def _envelope(self, line, source):
        return {"raw": line, "source": source, "sent_at_utc": now_utc(), "run_id": self.run_id}

    def _send_once(self, envelopes, partition_key):
        if self.fail_remaining > 0:
            self.fail_remaining -= 1
            raise ConnectionError("simulated transient failure")
        if self.client is None:
            return  # dry run
        from azure.eventhub import EventData
        batch = self.client.create_batch(partition_key=partition_key)
        for env in envelopes:
            batch.add(EventData(json.dumps(env, separators=(",", ":"))))
        self.client.send_batch(batch)

    def _send_with_retry(self, envelopes, partition_key):
        delays = self.cfg["retry_delays_s"]
        for attempt in range(len(delays) + 1):
            try:
                self._send_once(envelopes, partition_key)
                self.stats["events_sent"] += len(envelopes)
                return
            except Exception as exc:  # any send failure is treated as temporary until retries run out
                if attempt == len(delays):
                    self._dead_letter(envelopes, f"{type(exc).__name__}: {exc}", attempt)
                    return
                self.stats["retries"] += 1
                print(f"  send failed ({exc}); retry {attempt + 1}/{len(delays)} in {delays[attempt]}s")
                time.sleep(delays[attempt])

    def _dead_letter(self, envelopes, reason, retries):
        os.makedirs(os.path.dirname(self.dlq_path), exist_ok=True)
        with open(self.dlq_path, "a") as f:
            for env in envelopes:
                f.write(json.dumps({"raw": env["raw"], "source": env["source"], "reason": reason,
                                    "failed_at_utc": now_utc(), "retries": retries, "run_id": self.run_id}) + "\n")
        self.stats["events_dlq"] += len(envelopes)
        print(f"  gave up after {retries} retries -> {len(envelopes)} events to DLQ")

    def send_lines(self, lines, source="stream"):
        """Sends lines grouped by vehicle (partition key) so each vehicle's events stay in order."""
        started = time.time()
        groups = {}
        for line in lines:
            m = VEHICLE_RE.search(line)
            groups.setdefault(m.group(1) if m else None, []).append(line)
            self.stats["events_read"] += 1
        size = self.cfg["batch_max_events"]
        for key, group in groups.items():
            for i in range(0, len(group), size):
                self._send_with_retry([self._envelope(l, source) for l in group[i:i + size]], key)
        self._write_log(started, time.time(), source)
        return self.stats

    def _write_log(self, started, ended, source):
        duration = ended - started
        row = {"run_id": self.run_id, "source": source,
               "started_utc": datetime.fromtimestamp(started, timezone.utc).isoformat(),
               "duration_s": round(duration, 3), **self.stats,
               "events_per_s": round(self.stats["events_sent"] / duration, 1) if duration else None,
               "dry_run": self.client is None}
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        new = not os.path.exists(self.log_path)
        with open(self.log_path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(row))
            if new:
                w.writeheader()
            w.writerow(row)
        self.stats.update(duration_s=row["duration_s"], events_per_s=row["events_per_s"])

    def close(self):
        if self.client is not None:
            self.client.close()
