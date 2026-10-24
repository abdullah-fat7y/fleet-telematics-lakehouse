"""Send a file of raw events to the Fabric Eventstream.

    python send_stream.py                       # sends the sample file
    python send_stream.py --dry-run             # everything except the network call
    python send_stream.py --fail-first 6        # demo: retries with backoff, then DLQ

The connection string is read from the EVENTSTREAM_CONNECTION_STRING environment
variable or from a .env file next to this script. Never commit it to GitHub.
"""
import argparse
import json
import os

from fleet.common import load_config
from fleet.stream_sender import StreamSender

HERE = os.path.dirname(os.path.abspath(__file__))


def read_dotenv(path):
    if os.path.exists(path):
        for line in open(path):
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.strip().split("=", 1)
                os.environ.setdefault(key, value.strip().strip('"'))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=os.path.join(HERE, "config.yaml"))
    ap.add_argument("--file", help="JSON-lines file to send (default: the sample)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fail-first", type=int, default=0, help="simulate this many failed send attempts")
    args = ap.parse_args()

    cfg = load_config(args.config)
    read_dotenv(os.path.join(HERE, ".env"))
    conn = os.environ.get("EVENTSTREAM_CONNECTION_STRING")
    if not conn and not args.dry_run:
        raise SystemExit("Set EVENTSTREAM_CONNECTION_STRING in generator/.env (see README).")

    path = args.file or os.path.join(HERE, cfg["output"]["sample_dir"], "sample_events.jsonl")
    with open(path) as f:
        lines = [l.rstrip("\n") for l in f if l.strip()]

    sender = StreamSender(cfg, HERE, connection_string=conn, dry_run=args.dry_run, fail_first=args.fail_first)
    try:
        stats = sender.send_lines(lines)
    finally:
        sender.close()
    print(json.dumps({"run_id": sender.run_id, **stats}, indent=2))


if __name__ == "__main__":
    main()
