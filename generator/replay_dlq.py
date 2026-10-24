"""Recovery: resend events from the local dead-letter file to the Eventstream.

    python replay_dlq.py            # resend every DLQ entry not replayed yet
    python replay_dlq.py --dry-run  # show what would be resent

Replayed events reach bronze with source = "dlq_replay" and a new run_id. Each DLQ entry is
replayed once (its hash is recorded in output/dlq/replayed.txt). Notebook 04 then marks the
matching dlq_telemetry rows as recovered by finding the same raw text in bronze.
"""
import argparse
import hashlib
import json
import os

from fleet.common import load_config
from fleet.stream_sender import StreamSender
from send_stream import read_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))


def entry_hash(entry):
    return hashlib.sha1(f"{entry['run_id']}|{entry['failed_at_utc']}|{entry['raw']}".encode()).hexdigest()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=os.path.join(HERE, "config.yaml"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.config)
    dlq_path = os.path.join(HERE, cfg["output"]["send_dlq_file"])
    done_path = os.path.join(os.path.dirname(dlq_path), "replayed.txt")
    if not os.path.exists(dlq_path):
        raise SystemExit("No DLQ file yet - nothing to replay.")
    done = set(open(done_path).read().split()) if os.path.exists(done_path) else set()
    with open(dlq_path) as f:
        entries = [json.loads(line) for line in f if line.strip()]
    todo = [e for e in entries if entry_hash(e) not in done]
    print(f"DLQ entries: {len(entries)}, already replayed: {len(entries) - len(todo)}, to replay: {len(todo)}")
    if not todo:
        return

    read_dotenv(os.path.join(HERE, ".env"))
    conn = os.environ.get("EVENTSTREAM_CONNECTION_STRING")
    if not conn and not args.dry_run:
        raise SystemExit("Set EVENTSTREAM_CONNECTION_STRING in generator/.env (see README).")
    sender = StreamSender(cfg, HERE, connection_string=conn, dry_run=args.dry_run)
    try:
        stats = sender.send_lines([e["raw"] for e in todo], source="dlq_replay")
    finally:
        sender.close()
    if not args.dry_run:
        # Anything that failed again is already back in the DLQ file as a new entry.
        with open(done_path, "a") as f:
            f.writelines(entry_hash(e) + "\n" for e in todo)
    print(json.dumps({"replay_run_id": sender.run_id, "replayed_from_runs": sorted({e["run_id"] for e in todo}),
                      **stats}, indent=2))


if __name__ == "__main__":
    main()
