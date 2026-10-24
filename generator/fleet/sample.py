"""The ~1,000-event handoff sample for Person 2 (day 1-2 deliverable).

Same code path as the full generator, but small and with every anomaly and
bad-data type forced to appear at least a few times.
"""
import copy
import json
import os

from .bad_data import BAD_TYPES, DUPLICATE, LATE, corrupt
from .common import EVENT_FIELDS, rng_for, write_csv
from .reference import load_reference
from .simulate import COLD_CHAIN, DEVIATION, IDLING, plan_episodes, simulate_trip


def generate_sample(cfg, ref_dir, out_dir):
    sc = cfg["sample"]
    per_vehicle = sc["target_events"] // sc["vehicles"]
    # The sample keeps only the first ~25 minutes of each trip, so idling episodes are capped to fit.
    cfg = copy.deepcopy(cfg)
    dt = cfg["simulation"]["reading_interval_s"]
    idl = cfg["anomalies"][IDLING]
    idl["max_minutes"] = min(idl["max_minutes"], (per_vehicle - 10) * dt / 60)

    ref = load_reference(ref_dir)
    vehicles = sorted(ref["vehicles"].values(), key=lambda v: v["vehicle_id"])
    n_anom = sc["min_each_anomaly"]
    reefers = [v for v in vehicles if v["vehicle_type"] == "refrigerated_truck"][: max(n_anom, sc["vehicles"] * 3 // 10)]
    dry = [v for v in vehicles if v["vehicle_type"] == "dry_van"][: sc["vehicles"] - len(reefers)]
    # Who gets which forced anomaly: reefers get cold-chain breaches, dry vans idling / deviation.
    forced_for = {}
    for v in reefers[:n_anom]:
        forced_for[v["vehicle_id"]] = {COLD_CHAIN: 1}
    for v in dry[:n_anom]:
        forced_for[v["vehicle_id"]] = {IDLING: 1}
    for v in dry[n_anom: 2 * n_anom]:
        forced_for[v["vehicle_id"]] = {DEVIATION: 1}

    clean, truth, counter = [], [], {}
    for v in reefers + dry:
        rng = rng_for(cfg["seed"], "sample", v["vehicle_id"])
        trip = ref["trips"][v["vehicle_id"]][0]
        forced = dict(forced_for.get(v["vehicle_id"], {}), _max_readings=per_vehicle)
        episodes = plan_episodes(cfg, rng, v, trip, counter, forced=forced)
        path = ref["paths"][trip["corridor_id"]]
        for event, tag in simulate_trip(cfg, rng, v, trip, path, float(v["start_odometer_km"]), episodes,
                                        max_readings=per_vehicle):
            clean.append(event)
            if tag:
                truth.append({"event_id": event["event_id"], "anomaly_type": tag[0], "anomaly_group_id": tag[1]})

    # Bad data goes only on events that are not part of an injected anomaly, so the truth table stays valid.
    rng = rng_for(cfg["seed"], "sample", "bad")
    anomalous = {t["event_id"] for t in truth}
    candidates = [i for i, e in enumerate(clean) if e["event_id"] not in anomalous]
    picked = rng.sample(candidates, sc["min_each_bad_type"] * len(BAD_TYPES))
    bad_kind = {idx: BAD_TYPES[n % len(BAD_TYPES)] for n, idx in enumerate(picked)}

    # Emit like a live stream: all vehicles interleaved in time order, late events at the very end.
    order = sorted(range(len(clean)), key=lambda i: (clean[i]["event_time"], clean[i]["vehicle_id"]))
    lines, late, manifest = [], [], []
    for i in order:
        kind = bad_kind.get(i)
        if kind is None:
            lines.append(json.dumps(clean[i], separators=(",", ":")))
            continue
        raws, detail = corrupt(rng, clean[i], kind)
        manifest.append({"event_id": clean[i]["event_id"], "bad_type": kind, "detail": detail})
        (late if kind == LATE else lines).extend(raws)
    lines.extend(late)

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "sample_events.jsonl"), "w") as f:
        f.write("\n".join(lines) + "\n")
    write_csv(os.path.join(out_dir, "telemetry_truth.csv"), truth, ["event_id", "anomaly_type", "anomaly_group_id"])
    write_csv(os.path.join(out_dir, "bad_data_manifest.csv"), manifest, ["event_id", "bad_type", "detail"])

    summary = {
        "seed": cfg["seed"],
        "vehicles": len(reefers) + len(dry),
        "refrigerated_vehicles": len(reefers),
        "clean_readings_generated": len(clean),
        "lines_in_file": len(lines),
        "anomaly_episodes": {k: len({t["anomaly_group_id"] for t in truth if t["anomaly_type"] == k})
                             for k in (COLD_CHAIN, IDLING, DEVIATION)},
        "anomaly_readings": len(truth),
        "bad_data_events": {k: sum(m["bad_type"] == k for m in manifest) for k in BAD_TYPES},
        "extra_lines_from_duplicates": sum(m["bad_type"] == DUPLICATE for m in manifest),
        "fields": EVENT_FIELDS,
    }
    with open(os.path.join(out_dir, "sample_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    return summary
