"""Reference data: the description of the fleet, created once before any readings exist."""
import itertools
import math
import os
from datetime import timedelta

from .common import (Path, bearing_deg, destination, fmt_utc, haversine_km, parse_utc,
                     read_csv, rng_for, write_csv)

FIRST_NAMES = ["Ahmed", "Mohamed", "Mahmoud", "Omar", "Youssef", "Mostafa", "Karim", "Hassan",
               "Ali", "Tarek", "Amr", "Khaled", "Ibrahim", "Sherif", "Hany", "Walid", "Sara",
               "Mona", "Nour", "Heba", "Yasmin", "Salma", "Reem", "Dina"]
LAST_NAMES = ["Hassan", "Ibrahim", "Mahmoud", "Abdelrahman", "Saeed", "Fathy", "Ramadan",
              "Mansour", "Farouk", "Nabil", "Salem", "Gamal", "Shawky", "Zaki", "Fouad", "Kamel"]
RISK_LEVELS = [("low", 0.6), ("medium", 0.3), ("high", 0.1)]
MAKES = [("Mercedes-Benz", "Actros"), ("Volvo", "FH"), ("MAN", "TGX"), ("Scania", "R-series"),
         ("Isuzu", "FVR"), ("Mitsubishi Fuso", "Canter")]

DRIVER_FIELDS = ["driver_id", "full_name", "home_depot_id", "risk_level", "license_class",
                 "hire_date", "effective_from"]


def _weighted(rng, options):
    r, acc = rng.random(), 0.0
    for value, weight in options:
        acc += weight
        if r < acc:
            return value
    return options[-1][0]


def build_depots(cfg):
    return [dict(d) for d in cfg["depots"]]


def build_corridors(cfg):
    """Fixed routes between depot pairs, with GPS points ~1 km apart.

    Real roads are not straight lines, so each corridor bows away from the straight
    line by a random amount and has a gentle wiggle.
    """
    rng = rng_for(cfg["seed"], "corridors")
    cc = cfg["corridors"]
    depots = {d["depot_id"]: d for d in cfg["depots"]}
    pairs = list(itertools.combinations(sorted(depots), 2))
    rng.shuffle(pairs)

    # Every depot gets at least one corridor, then fill up to the requested count.
    chosen = []
    for dep in sorted(depots):
        if not any(dep in p for p in chosen):
            chosen.append(next(p for p in pairs if dep in p and p not in chosen))
    for p in pairs:
        if len(chosen) >= cc["count"]:
            break
        if p not in chosen:
            chosen.append(p)

    corridors, points = [], []
    for i, (a, b) in enumerate(chosen, start=1):
        da, db = depots[a], depots[b]
        straight = haversine_km(da["lat"], da["lon"], db["lat"], db["lon"])
        heading = bearing_deg(da["lat"], da["lon"], db["lat"], db["lon"])
        bend = rng.uniform(-cc["max_bend_km"], cc["max_bend_km"]) * min(1.0, straight / 100)
        wiggle, wiggle_waves = rng.uniform(0.2, 0.8), rng.randint(2, 6)
        n = max(2, round(straight * 1.03 / cc["point_spacing_km"]))
        pts = []
        for k in range(n + 1):
            t = k / n
            lat = da["lat"] + (db["lat"] - da["lat"]) * t
            lon = da["lon"] + (db["lon"] - da["lon"]) * t
            offset = bend * math.sin(math.pi * t) + wiggle * math.sin(wiggle_waves * math.pi * t)
            pts.append(destination(lat, lon, heading + 90, offset))
        path = Path(pts)
        cid = f"COR-{i:03d}"
        corridors.append({
            "corridor_id": cid,
            "name": f"{da['name']} - {db['name']}",
            "origin_depot_id": a,
            "dest_depot_id": b,
            "planned_distance_km": round(path.length_km, 3),
            "point_count": len(pts),
            "reefer_temp_min_c": cc["reefer_temp_min_c"],
            "reefer_temp_max_c": cc["reefer_temp_max_c"],
        })
        for seq, (lat, lon) in enumerate(pts):
            points.append({"corridor_id": cid, "seq": seq, "latitude": round(lat, 6),
                           "longitude": round(lon, 6), "km_from_start": round(path.cum[seq], 3)})
    return corridors, points


def build_drivers(cfg):
    rng = rng_for(cfg["seed"], "drivers")
    depot_ids = [d["depot_id"] for d in cfg["depots"]]
    drivers = []
    for i in range(1, cfg["fleet"]["drivers"] + 1):
        hire = parse_utc("2015-01-01T00:00:00Z") + timedelta(days=rng.randint(0, 3900))
        drivers.append({
            "driver_id": f"DRV-{i:05d}",
            "full_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
            "home_depot_id": rng.choice(depot_ids),
            "risk_level": _weighted(rng, RISK_LEVELS),
            "license_class": rng.choice(["C", "CE"]),
            "hire_date": hire.date().isoformat(),
            "effective_from": "2026-01-01T00:00:00Z",
        })
    return drivers


def build_drivers_v2(cfg, drivers):
    """Second version of the drivers file: some drivers changed depot or risk level.

    The change takes effect in the middle of the simulation, so Person 3's facts
    split across two driver versions (that is what makes SCD Type 2 visible).
    """
    rng = rng_for(cfg["seed"], "drivers_v2")
    depot_ids = [d["depot_id"] for d in cfg["depots"]]
    sim_start = parse_utc(cfg["simulation"]["start_time_utc"])
    change_at = fmt_utc(sim_start + timedelta(hours=cfg["simulation"]["hours"] / 2))
    changed = set(rng.sample(range(len(drivers)), round(len(drivers) * cfg["fleet"]["driver_changes_share"])))
    v2 = []
    for i, d in enumerate(drivers):
        row = dict(d)
        if i in changed:
            what = rng.choice(["depot", "risk", "both"])
            if what in ("depot", "both"):
                row["home_depot_id"] = rng.choice([x for x in depot_ids if x != d["home_depot_id"]])
            if what in ("risk", "both"):
                row["risk_level"] = rng.choice([r for r, _ in RISK_LEVELS if r != d["risk_level"]])
            row["effective_from"] = change_at
        v2.append(row)
    return v2


def build_vehicles(cfg, drivers):
    rng = rng_for(cfg["seed"], "vehicles")
    n = cfg["fleet"]["vehicles"]
    n_reefer = round(n * cfg["fleet"]["refrigerated_share"])
    types = ["refrigerated_truck"] * n_reefer + ["dry_van"] * (n - n_reefer)
    rng.shuffle(types)
    vb = cfg["vehicle_behaviour"]
    vehicles = []
    for i in range(n):
        driver = drivers[i]  # one driver per vehicle; the extra drivers are spares
        make, model = rng.choice(MAKES)
        vehicles.append({
            "vehicle_id": f"VEH-{i + 1:05d}",
            "vehicle_type": types[i],
            "make": make,
            "model": model,
            "model_year": rng.randint(2014, 2025),
            "home_depot_id": driver["home_depot_id"],
            "assigned_driver_id": driver["driver_id"],
            "start_odometer_km": round(rng.uniform(vb["odometer_start_min_km"], vb["odometer_start_max_km"]), 1),
        })
    return vehicles


def build_trips(cfg, vehicles, corridors):
    """Each vehicle drives out from its home depot and back, alternating direction."""
    rng = rng_for(cfg["seed"], "trips")
    vb = cfg["vehicle_behaviour"]
    sim_start = parse_utc(cfg["simulation"]["start_time_utc"])
    interval = cfg["simulation"]["reading_interval_s"]
    by_depot = {}
    for c in corridors:
        by_depot.setdefault(c["origin_depot_id"], []).append((c, "forward"))
        by_depot.setdefault(c["dest_depot_id"], []).append((c, "reverse"))

    # Day 1 is generated first with the same random draws as before, so its trips (and the pilot
    # data built on them) never change; later days are appended with continuing trip numbers.
    trips, n = [], 0
    for day in range(cfg["simulation"].get("days", 1)):
        for v in vehicles:
            corridor, direction = rng.choice(by_depot[v["home_depot_id"]])
            start = sim_start + timedelta(days=day, seconds=interval * rng.randint(0, 180))  # first 90 minutes
            for leg in range(cfg["fleet"]["trips_per_vehicle"]):
                n += 1
                d = direction if leg % 2 == 0 else ("reverse" if direction == "forward" else "forward")
                origin, dest = ((corridor["origin_depot_id"], corridor["dest_depot_id"]) if d == "forward"
                                else (corridor["dest_depot_id"], corridor["origin_depot_id"]))
                drive_min = corridor["planned_distance_km"] / vb["cruise_speed_kmh"] * 60
                stops = int(drive_min // vb["planned_stop_every_min"])
                end = start + timedelta(minutes=drive_min + stops * vb["planned_stop_minutes"])
                trips.append({
                    "trip_id": f"TRP-{n:06d}",
                    "vehicle_id": v["vehicle_id"],
                    "driver_id": v["assigned_driver_id"],
                    "corridor_id": corridor["corridor_id"],
                    "direction": d,
                    "origin_depot_id": origin,
                    "dest_depot_id": dest,
                    "planned_distance_km": corridor["planned_distance_km"],
                    "planned_start_utc": fmt_utc(start),
                    "planned_end_utc": fmt_utc(end),
                })
                # Turnaround at the destination before driving back.
                start = end + timedelta(seconds=interval * rng.randint(60, 180))
    return trips


def generate_reference(cfg, out_dir):
    depots = build_depots(cfg)
    corridors, points = build_corridors(cfg)
    drivers = build_drivers(cfg)
    drivers_v2 = build_drivers_v2(cfg, drivers)
    vehicles = build_vehicles(cfg, drivers)
    trips = build_trips(cfg, vehicles, corridors)

    write_csv(os.path.join(out_dir, "depots.csv"), depots, ["depot_id", "name", "lat", "lon"])
    write_csv(os.path.join(out_dir, "corridors.csv"), corridors, list(corridors[0]))
    write_csv(os.path.join(out_dir, "corridor_points.csv"), points, list(points[0]))
    write_csv(os.path.join(out_dir, "drivers_v1.csv"), drivers, DRIVER_FIELDS)
    write_csv(os.path.join(out_dir, "drivers_v2.csv"), drivers_v2, DRIVER_FIELDS)
    write_csv(os.path.join(out_dir, "vehicles.csv"), vehicles, list(vehicles[0]))
    write_csv(os.path.join(out_dir, "trips.csv"), trips, list(trips[0]))
    return {"depots": len(depots), "corridors": len(corridors), "corridor_points": len(points),
            "drivers": len(drivers), "drivers_changed_in_v2": sum(a != b for a, b in zip(drivers, drivers_v2)),
            "vehicles": len(vehicles), "trips": len(trips)}


def load_reference(ref_dir):
    """Reads the reference CSVs back into lookups used by the telemetry simulator."""
    corridors = {c["corridor_id"]: c for c in read_csv(os.path.join(ref_dir, "corridors.csv"))}
    pts = {}
    for p in read_csv(os.path.join(ref_dir, "corridor_points.csv")):
        pts.setdefault(p["corridor_id"], []).append((int(p["seq"]), float(p["latitude"]), float(p["longitude"])))
    paths = {cid: Path([(lat, lon) for _, lat, lon in sorted(v)]) for cid, v in pts.items()}
    vehicles = {v["vehicle_id"]: v for v in read_csv(os.path.join(ref_dir, "vehicles.csv"))}
    trips = {}
    for t in read_csv(os.path.join(ref_dir, "trips.csv")):
        trips.setdefault(t["vehicle_id"], []).append(t)
    return {"corridors": corridors, "paths": paths, "vehicles": vehicles, "trips": trips}
