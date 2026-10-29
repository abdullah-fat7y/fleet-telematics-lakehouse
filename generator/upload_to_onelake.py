"""Upload files from this machine to the Lakehouse in OneLake.

    python upload_to_onelake.py landing          # new batch files -> Files/landing/<run_id>/
    python upload_to_onelake.py landing --watch  # keep uploading new files every 30 s (Ctrl+C to stop)
    python upload_to_onelake.py ops              # DLQ file + sender logs -> Files/ops/ (for notebook 04)
    python upload_to_onelake.py reference        # reference CSVs -> Files/reference/
    python upload_to_onelake.py truth            # truth table of every v2 run -> Files/truth_v2/<run_id>.csv

Sign-in: a browser window opens once; sign in with the account that has access to the workspace.
FABRIC_TENANT_ID must be set in generator/.env (the workspace's tenant; never commit it).
Each file is uploaded under a temporary name and renamed when complete, so the Fabric notebook
never reads a half-uploaded file. Failed uploads are retried after 1, 2, 4, 8 s and otherwise
left for the next round - the local file is kept, so nothing is lost.
"""
import argparse
import glob
import os
import time

from fleet.common import load_config
from send_stream import read_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))


def connect(cfg):
    from azure.identity import InteractiveBrowserCredential, TokenCachePersistenceOptions
    from azure.storage.filedatalake import DataLakeServiceClient

    tenant = os.environ.get("FABRIC_TENANT_ID")
    if not tenant:
        raise SystemExit("Set FABRIC_TENANT_ID in generator/.env (the Fabric workspace's tenant ID).")
    try:  # remember the sign-in in the macOS keychain so later runs do not ask again
        cred = InteractiveBrowserCredential(tenant_id=tenant,
                                            cache_persistence_options=TokenCachePersistenceOptions(name="fleet-onelake"))
    except Exception:
        cred = InteractiveBrowserCredential(tenant_id=tenant)
    oc = cfg["onelake"]
    # Generous timeouts: on a slow home connection a 4 MB chunk can take a while.
    fs = DataLakeServiceClient(oc["account_url"], credential=cred, connection_timeout=60,
                               read_timeout=600).get_file_system_client(oc["workspace"])
    return fs, f"{oc['lakehouse']}.Lakehouse/Files"


def upload(fs, local_path, remote_path, delays):
    for attempt in range(len(delays) + 1):
        try:
            tmp = fs.get_file_client(remote_path + ".uploading")
            with open(local_path, "rb") as f:
                # 4 MB chunks, 4 in parallel: a large file never depends on one long request.
                tmp.upload_data(f, overwrite=True, chunk_size=4 * 1024 * 1024, max_concurrency=4)
            tmp.rename_file(f"{fs.file_system_name}/{remote_path}")
            return True
        except Exception as exc:
            if attempt == len(delays):
                print(f"  upload failed for {os.path.basename(local_path)}: {exc} (will retry next round)")
                return False
            print(f"  upload failed ({type(exc).__name__}); retry {attempt + 1}/{len(delays)} in {delays[attempt]}s")
            time.sleep(delays[attempt])


def upload_landing(cfg, fs, root):
    landing = os.path.join(HERE, cfg["output"]["landing_dir"])
    done_path = os.path.join(landing, "uploaded.txt")
    done = set(open(done_path).read().split()) if os.path.exists(done_path) else set()
    todo = [p for p in sorted(glob.glob(os.path.join(landing, "*", "*.jsonl")))
            if os.path.relpath(p, landing) not in done]
    ok = 0
    for p in todo:
        rel = os.path.relpath(p, landing)
        if upload(fs, p, f"{root}/landing/{rel}", cfg["streaming"]["retry_delays_s"]):
            with open(done_path, "a") as f:
                f.write(rel + "\n")
            ok += 1
    print(f"landing: {ok} of {len(todo)} new files uploaded")
    return ok


def upload_ops(cfg, fs, root):
    out = cfg["output"]
    for key in ("send_dlq_file", "send_log_file", "interval_log_file"):
        p = os.path.join(HERE, out[key])
        if os.path.exists(p):
            ok = upload(fs, p, f"{root}/ops/{os.path.basename(p)}", cfg["streaming"]["retry_delays_s"])
            print(f"ops: {os.path.basename(p)} {'uploaded' if ok else 'FAILED'}")


def upload_reference(cfg, fs, root):
    """Reference CSVs (e.g. trips.csv after adding a day) -> Files/reference/."""
    ref = os.path.join(HERE, cfg["output"]["reference_dir"])
    for p in sorted(glob.glob(os.path.join(ref, "*.csv"))):
        ok = upload(fs, p, f"{root}/reference/{os.path.basename(p)}", cfg["streaming"]["retry_delays_s"])
        print(f"reference: {os.path.basename(p)} {'uploaded' if ok else 'FAILED'}")


def upload_truth(cfg, fs, root):
    """Truth tables of every v2 run (run ids starting with api-) -> Files/truth_v2/<run_id>.csv."""
    runs = os.path.join(HERE, cfg["output"]["runs_dir"])
    for p in sorted(glob.glob(os.path.join(runs, "api-*", "telemetry_truth.csv"))):
        run_id = os.path.basename(os.path.dirname(p))
        ok = upload(fs, p, f"{root}/truth_v2/{run_id}.csv", cfg["streaming"]["retry_delays_s"])
        print(f"truth: {run_id} {'uploaded' if ok else 'FAILED'}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("what", choices=["landing", "ops", "reference", "truth"])
    ap.add_argument("--watch", action="store_true", help="landing only: keep uploading on a schedule")
    ap.add_argument("--config", default=os.path.join(HERE, "config.yaml"))
    args = ap.parse_args()

    cfg = load_config(args.config)
    read_dotenv(os.path.join(HERE, ".env"))
    fs, root = connect(cfg)
    if args.what in ("ops", "reference", "truth"):
        {"ops": upload_ops, "reference": upload_reference, "truth": upload_truth}[args.what](cfg, fs, root)
        return
    upload_landing(cfg, fs, root)
    while args.watch:
        time.sleep(cfg["batch"]["upload_every_s"])
        upload_landing(cfg, fs, root)


if __name__ == "__main__":
    main()
