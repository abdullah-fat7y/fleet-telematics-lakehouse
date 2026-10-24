"""Full fleet run: generate every vehicle's readings and stream them to the Eventstream.

    python run_fleet.py                          # all 5,000 vehicles, hours from config.yaml
    python run_fleet.py --hours 2                # all vehicles, first 2 simulated hours
    python run_fleet.py --vehicles 50 --hours 0.25   # small smoke test
    python run_fleet.py --dry-run                # generate only, measure generator speed
    python run_fleet.py --vehicles 20 --from-hours 2 --hours 2.25 --workers 1 --fail-first 6
                                                 # retry demo: new data, retries with backoff, then DLQ

    python run_fleet.py --mode batch --from-hours 2 --hours 2.5
                                                 # batch path: files in output/landing/<run_id>/ for Files/landing

Outputs per run in output/runs/<run_id>/: telemetry_truth.csv, bad_data_manifest.csv, perf.json.
Logs: output/logs/ingestion_intervals.csv (one row per interval) and output/logs/send_log.csv.
"""
import argparse
import asyncio
import json
import os

from fleet.common import load_config
from fleet.fleet_run import run_fleet
from send_stream import read_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=os.path.join(HERE, "config.yaml"))
    ap.add_argument("--hours", type=float, help="simulate up to this hour (default: config)")
    ap.add_argument("--from-hours", type=float, default=0.0,
                    help="only send readings from this simulated hour on (e.g. new data for a demo)")
    ap.add_argument("--vehicles", type=int, help="only the first N vehicles (default: all)")
    ap.add_argument("--workers", type=int, help="number of async workers (default: config)")
    ap.add_argument("--mode", choices=["stream", "batch"], default="stream",
                    help="stream = send to the Eventstream; batch = write files for Files/landing")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fail-first", type=int, default=0)
    args = ap.parse_args()

    cfg = load_config(args.config)
    read_dotenv(os.path.join(HERE, ".env"))
    conn = os.environ.get("EVENTSTREAM_CONNECTION_STRING")
    if not conn and not args.dry_run and args.mode == "stream":
        raise SystemExit("Set EVENTSTREAM_CONNECTION_STRING in generator/.env (see README).")
    perf = asyncio.run(run_fleet(cfg, HERE, conn, hours=args.hours, max_vehicles=args.vehicles,
                                 workers=args.workers, fail_first=args.fail_first, dry_run=args.dry_run,
                                 from_hours=args.from_hours, mode=args.mode))
    print(json.dumps(perf, indent=2))


if __name__ == "__main__":
    main()
