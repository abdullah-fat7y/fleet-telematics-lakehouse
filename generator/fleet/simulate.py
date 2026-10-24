"""Normal telemetry readings for one trip, with the three anomalies injected on schedule."""
import math
from datetime import timedelta

from .common import destination, fmt_utc, parse_utc, poisson, seeded_uuid

COLD_CHAIN = "cold_chain_breach"
IDLING = "excessive_idling"
DEVIATION = "route_deviation"
GROUP_PREFIX = {COLD_CHAIN: "CCB", IDLING: "IDL", DEVIATION: "DEV"}


def is_planned_stop(cfg, k):
    """Planned stops happen by reading index: the last few readings of every 2-hour block."""
    vb, dt = cfg["vehicle_behaviour"], cfg["simulation"]["reading_interval_s"]
    every = vb["planned_stop_every_min"] * 60 // dt
    length = vb["planned_stop_minutes"] * 60 // dt
    return k % every >= every - length


def episode_length(cfg, rng, kind):
    a, dt = cfg["anomalies"][kind], cfg["simulation"]["reading_interval_s"]
    if kind == IDLING:
        return math.ceil(rng.uniform(a["min_minutes"], a["max_minutes"]) * 60 / dt)
    return rng.randint(a["min_readings"], a["max_readings"])


def plan_episodes(cfg, rng, vehicle, trip, counter, forced=None):
    """Decide where in this trip each anomaly goes (reading index + length).

    `forced` (used by the sample) overrides the random counts, e.g. {"excessive_idling": 1}.
    Episodes never overlap each other or a planned stop, and skip the start-up ramp.
    """
    vb, dt = cfg["vehicle_behaviour"], cfg["simulation"]["reading_interval_s"]
    est_readings = int(float(trip["planned_distance_km"]) / vb["cruise_speed_kmh"] * 3600 / dt)
    hours = est_readings * dt / 3600
    reefer = vehicle["vehicle_type"] == "refrigerated_truck"
    a = cfg["anomalies"]
    if forced is not None:
        counts = forced
    else:
        counts = {
            COLD_CHAIN: poisson(rng, a[COLD_CHAIN]["rate_per_reefer_hour"] * hours) if reefer else 0,
            IDLING: poisson(rng, a[IDLING]["rate_per_vehicle_hour"] * hours),
            DEVIATION: poisson(rng, a[DEVIATION]["rate_per_vehicle_hour"] * hours),
        }
    taken, episodes = set(), []
    lo = vb["ramp_up_readings"] + 1
    for kind in (IDLING, COLD_CHAIN, DEVIATION):
        for _ in range(counts.get(kind, 0)):
            n = episode_length(cfg, rng, kind)
            for _attempt in range(50):
                hi = min(est_readings, forced_limit(forced, est_readings)) - n - 2
                if hi <= lo:
                    break
                start = rng.randint(lo, hi)
                span = range(start - 1, start + n + 1)  # keep a one-reading gap between episodes
                if any(k in taken or is_planned_stop(cfg, k) for k in span):
                    continue
                taken.update(span)
                counter[kind] = counter.get(kind, 0) + 1
                episodes.append({
                    "kind": kind, "start": start, "n": n,
                    "group_id": f"{GROUP_PREFIX[kind]}-{vehicle['vehicle_id']}-{counter[kind]:03d}",
                    "side": rng.choice([-1, 1]),
                    "peak": rng.uniform(a[COLD_CHAIN]["breach_temp_min_c"], a[COLD_CHAIN]["breach_temp_max_c"]),
                    "offset_km": rng.uniform(a[DEVIATION]["offset_min_km"], a[DEVIATION]["offset_max_km"]),
                })
                break
    return episodes


def forced_limit(forced, est_readings):
    # The sample only keeps the first readings of a trip, so forced episodes must fit early.
    return forced.get("_max_readings", est_readings) if forced else est_readings


def simulate_trip(cfg, rng, vehicle, trip, path, odometer_km, episodes, max_readings=None, end_time=None):
    """Yields (event, truth) per reading. truth is None or (anomaly_type, group_id).

    Readings are only produced while the vehicle is on a trip (the telematics unit
    reports with the ignition key in; planned stops still report, with rpm 0).
    Returns the final odometer so the next trip continues from it.
    """
    vb, dt = cfg["vehicle_behaviour"], cfg["simulation"]["reading_interval_s"]
    if trip["direction"] == "reverse":
        path = path.reversed()
    reefer = vehicle["vehicle_type"] == "refrigerated_truck"
    t = parse_utc(trip["planned_start_utc"])
    # engine_base is the noise-free temperature; noise is added only to the emitted value.
    dist, prev_speed, engine_base = 0.0, 0.0, float(vb["engine_start_temp_c"])
    warmup_step = (vb["engine_operating_temp_c"] - vb["engine_start_temp_c"]) / (vb["engine_warmup_minutes"] * 60 / dt)
    k = 0
    while True:
        if max_readings is not None and k >= max_readings:
            break
        if end_time is not None and t >= end_time:
            break
        active = next((e for e in episodes if e["start"] <= k < e["start"] + e["n"]), None)
        kind = active["kind"] if active else None

        # Move by the distance covered since the previous reading.
        moved = prev_speed * dt / 3600
        dist = min(dist + moved, path.length_km)
        odometer_km += moved
        arrived = dist >= path.length_km

        stopped = is_planned_stop(cfg, k) and kind is None
        if arrived or stopped:
            speed, rpm, fuel = 0.0, 0, 0.0
            engine_base = max(vb["engine_start_temp_c"], engine_base - vb["engine_cooling_per_reading_c"])
        else:
            if kind == IDLING:
                speed = 0.0  # stuck with the engine running
            else:
                ramp = min(1.0, (k + 1) / vb["ramp_up_readings"])
                speed = max(0.0, rng.gauss(vb["cruise_speed_kmh"], vb["speed_noise_kmh"]) * ramp)
            rpm = int(min(vb["max_rpm"], vb["idle_rpm"] + speed * vb["rpm_per_kmh"]) + rng.gauss(0, vb["rpm_noise"]))
            fuel = max(0.5, vb["fuel_idle_lph"] + speed * vb["fuel_per_kmh_lph"] + rng.gauss(0, vb["fuel_noise_lph"]))
            engine_base = min(vb["engine_operating_temp_c"], engine_base + warmup_step)
        engine_temp = engine_base + rng.gauss(0, vb["engine_temp_noise_c"])

        lat, lon, heading = path.position_at(dist)
        if kind == DEVIATION:
            lat, lon = destination(lat, lon, heading + 90 * active["side"], active["offset_km"])

        cargo = None
        if reefer:
            cargo = rng.gauss(vb["cargo_setpoint_c"], vb["cargo_noise_c"])
            if kind == COLD_CHAIN:
                # Rise towards the peak and fall back, but every reading stays above the limit.
                phase = math.sin(math.pi * (k - active["start"] + 0.5) / active["n"])
                low = cfg["anomalies"][COLD_CHAIN]["breach_temp_min_c"]
                cargo = low + (active["peak"] - low) * phase + abs(rng.gauss(0, 0.2))

        event = {
            "event_id": seeded_uuid(rng),
            "vehicle_id": vehicle["vehicle_id"],
            "trip_id": trip["trip_id"],
            "event_time": fmt_utc(t),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "speed_kmh": round(speed, 1),
            "rpm": max(0, rpm),
            "engine_temp_c": round(engine_temp, 1),
            "fuel_rate_lph": round(fuel, 2),
            "odometer_km": round(odometer_km, 3),
            "cargo_temp_c": None if cargo is None else round(cargo, 1),
        }
        yield event, ((kind, active["group_id"]) if active else None)

        if arrived:
            break
        prev_speed = speed
        t += timedelta(seconds=dt)
        k += 1
    return odometer_km
