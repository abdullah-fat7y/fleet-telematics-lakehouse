"""Fleet telematics generator.

    python generate.py reference   # fleet description -> output/Files/reference
    python generate.py sample      # ~1,000-event handoff sample -> output/Files/sample
    python generate.py all         # both

Everything is driven by config.yaml; the same seed always gives the same files.
"""
import argparse
import json
import os

from fleet.common import load_config
from fleet.reference import generate_reference
from fleet.sample import generate_sample

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("what", choices=["reference", "sample", "all"])
    ap.add_argument("--config", default=os.path.join(HERE, "config.yaml"))
    args = ap.parse_args()

    cfg = load_config(args.config)
    ref_dir = os.path.join(HERE, cfg["output"]["reference_dir"])
    if args.what in ("reference", "all"):
        print("reference:", json.dumps(generate_reference(cfg, ref_dir), indent=2))
    if args.what in ("sample", "all"):
        print("sample:", json.dumps(generate_sample(cfg, ref_dir, os.path.join(HERE, cfg["output"]["sample_dir"])), indent=2))


if __name__ == "__main__":
    main()
