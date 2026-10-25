"""Kafka -> cloud bridge.

Reads the CDC topic (every new row of telemetry_live, written by Debezium) from the local Kafka and
forwards it to the Fabric Eventstream through its Kafka endpoint (SASL_SSL, the Eventstream connection
string is the password). Each message becomes one envelope in bronze_telemetry_v2:

    {"raw", "source": "kafka_cdc", "sent_at_utc", "run_id", "db_id", "is_valid", "reject_reason",
     "db_received_at", "kafka_partition", "kafka_offset"}

Delivery is at-least-once: Kafka offsets are committed only after the cloud has acknowledged the batch,
so a crash or restart never loses a row (a row may be re-sent; db_id identifies duplicates downstream).
Failed sends are retried after 1, 2, 4, 8 s, then written to logs/bridge_dlq.jsonl with the reason.
"""
import csv
import json
import os
import re
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, TopicPartition

RETRY_DELAYS_S = [1, 2, 4, 8]
LOG_EVERY_S = 10


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def eventstream_producer(conn):
    namespace = re.search(r"Endpoint=sb://([^/;]+)", conn).group(1)
    topic = re.search(r"EntityPath=([^;]+)", conn).group(1)
    producer = Producer({
        "bootstrap.servers": f"{namespace}:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "$ConnectionString",
        "sasl.password": conn,
        "acks": "all",
        "linger.ms": 50,
        "request.timeout.ms": 60000,
        "message.timeout.ms": 120000,
    })
    return producer, topic


class Bridge:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
            "group.id": "fleet-cloud-bridge",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        })
        self.source_topic = os.environ["SOURCE_TOPIC"]
        self.consumer.subscribe([self.source_topic])
        self.producer, self.cloud_topic = eventstream_producer(os.environ["EVENTSTREAM_V2_CONNECTION_STRING"])
        self.running = True
        self.stats = {"consumed": 0, "delivered": 0, "dlq": 0, "retries": 0}
        os.makedirs("logs", exist_ok=True)

    def envelope(self, msg):
        row = json.loads(msg.value())
        return row.get("vehicle_id"), json.dumps({
            "raw": row["raw"], "source": "kafka_cdc", "sent_at_utc": now_utc(), "run_id": row.get("run_id"),
            "db_id": row["id"], "is_valid": row["is_valid"], "reject_reason": row.get("reject_reason"),
            "db_received_at": row.get("received_at"),
            "kafka_partition": msg.partition(), "kafka_offset": msg.offset(),
        }, separators=(",", ":"))

    def deliver(self, items):
        """Sends (key, value) pairs; returns the ones that failed after all retries."""
        pending = items
        for attempt in range(len(RETRY_DELAYS_S) + 1):
            outcome = {}  # position -> None (acknowledged) or error text

            for i, (key, value) in enumerate(pending):
                self.producer.produce(self.cloud_topic, value=value, key=key,
                                      on_delivery=lambda err, _m, i=i: outcome.__setitem__(i, err and str(err)))
            self.producer.flush(120)
            # Anything the cloud did not explicitly acknowledge (e.g. the flush timed out) counts as failed,
            # so its Kafka offset is never committed for a message that did not arrive.
            failed = [(kv, outcome.get(i, "no acknowledgement before timeout"))
                      for i, kv in enumerate(pending) if outcome.get(i, "missing") is not None]
            if not failed:
                return []
            if attempt == len(RETRY_DELAYS_S):
                return failed
            self.stats["retries"] += 1
            print(f"{len(failed)} messages failed ({failed[0][1]}); retry {attempt + 1} in {RETRY_DELAYS_S[attempt]}s",
                  flush=True)
            time.sleep(RETRY_DELAYS_S[attempt])
            pending = [kv for kv, _ in failed]

    def lag(self):
        """Messages in Kafka not yet delivered: log end minus the committed offset (the position is
        unknown right after a restart, the committed offset is not)."""
        assigned = self.consumer.assignment()
        if not assigned:
            return None
        total = 0
        for tp in self.consumer.committed(assigned, timeout=5):
            low, high = self.consumer.get_watermark_offsets(tp, timeout=5)
            total += high - (tp.offset if tp.offset >= 0 else low)
        return total

    def run(self):
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "running", False))
        last_log, last = time.time(), dict(self.stats)
        new_log = not os.path.exists("logs/bridge_intervals.csv")
        with open("logs/bridge_intervals.csv", "a", newline="") as f:
            w = csv.writer(f)
            if new_log:
                w.writerow(["interval_end_utc", "consumed", "delivered", "dlq", "retries", "events_per_s", "kafka_lag"])
            while self.running:
                msgs = [m for m in self.consumer.consume(num_messages=1000, timeout=1.0) if not m.error()]
                if msgs:
                    self.stats["consumed"] += len(msgs)
                    failed = self.deliver([self.envelope(m) for m in msgs])
                    if failed:
                        with open("logs/bridge_dlq.jsonl", "a") as d:
                            for (_, value), reason in failed:
                                d.write(json.dumps({"envelope": value, "reason": reason, "failed_at_utc": now_utc()}) + "\n")
                        self.stats["dlq"] += len(failed)
                    self.stats["delivered"] += len(msgs) - len(failed)
                    self.consumer.commit(asynchronous=False)  # only after the cloud acknowledged
                if time.time() - last_log >= LOG_EVERY_S:
                    elapsed = time.time() - last_log
                    delivered = self.stats["delivered"] - last["delivered"]
                    row = [now_utc(), self.stats["consumed"] - last["consumed"], delivered,
                           self.stats["dlq"] - last["dlq"], self.stats["retries"] - last["retries"],
                           round(delivered / elapsed, 1), self.lag()]
                    w.writerow(row)
                    f.flush()
                    print(f"delivered {self.stats['delivered']:,} total, {row[5]:,} ev/s, kafka lag {row[6]}", flush=True)
                    last_log, last = time.time(), dict(self.stats)
        self.consumer.close()
        print("bridge stopped:", self.stats, flush=True)


if __name__ == "__main__":
    Bridge().run()
