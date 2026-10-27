"""Run a Fabric notebook on demand and wait for it to finish (Fabric REST API, Job Scheduler).

    python run_notebook.py 11_load_landing_v2

Uses the same interactive sign-in as upload_to_onelake.py (token cached in the macOS keychain).
This is the call an orchestrator (Airflow) makes to run a pipeline step.
"""
import argparse
import os
import sys
import time

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
GENERATOR = os.path.join(HERE, "..", "..", "generator")
sys.path.insert(0, GENERATOR)
from send_stream import read_dotenv  # noqa: E402

API = "https://api.fabric.microsoft.com/v1"


def fabric_token():
    from azure.identity import InteractiveBrowserCredential, TokenCachePersistenceOptions
    cred = InteractiveBrowserCredential(tenant_id=os.environ["FABRIC_TENANT_ID"],
                                        cache_persistence_options=TokenCachePersistenceOptions(name="fleet-onelake"))
    return cred.get_token("https://api.fabric.microsoft.com/.default").token


def run_notebook(name, poll_s=15, timeout_s=3600):
    read_dotenv(os.path.join(GENERATOR, ".env"))
    ws = os.environ["FABRIC_WORKSPACE_ID"]
    headers = {"Authorization": f"Bearer {fabric_token()}"}
    with httpx.Client(headers=headers, timeout=60) as http:
        items = http.get(f"{API}/workspaces/{ws}/items", params={"type": "Notebook"}).json()["value"]
        item = next((i for i in items if i["displayName"] == name), None)
        if item is None:
            raise SystemExit(f"notebook {name!r} not found in the workspace")
        r = http.post(f"{API}/workspaces/{ws}/items/{item['id']}/jobs/instances", params={"jobType": "RunNotebook"})
        if r.status_code != 202:
            raise SystemExit(f"could not start {name}: {r.status_code} {r.text[:300]}")
        job_url, started = r.headers["Location"], time.time()
        print(f"started {name} (job {job_url.rsplit('/', 1)[-1]})", flush=True)
        while time.time() - started < timeout_s:
            time.sleep(poll_s)
            job = http.get(job_url).json()
            status = job.get("status")
            print(f"  {time.time() - started:5.0f} s  {status}", flush=True)
            if status in ("Completed", "Failed", "Cancelled", "Deduped"):
                if status != "Completed":
                    raise SystemExit(f"{name} {status}: {job.get('failureReason')}")
                return job
        raise SystemExit(f"{name} still running after {timeout_s} s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("notebook")
    run_notebook(ap.parse_args().notebook)
