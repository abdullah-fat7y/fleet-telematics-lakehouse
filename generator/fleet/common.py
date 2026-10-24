"""Shared helpers: config loading, seeded randomness, time and GPS maths."""
import bisect
import csv
import math
import os
import random
import uuid
from datetime import datetime, timezone

import yaml

EARTH_RADIUS_KM = 6371.0088

# Field order of one telemetry event (Section 0 of the work plan). Never rename these.
EVENT_FIELDS = [
    "event_id", "vehicle_id", "trip_id", "event_time", "latitude", "longitude",
    "speed_kmh", "rpm", "engine_temp_c", "fuel_rate_lph", "odometer_km", "cargo_temp_c",
]
# Fields that must never be empty. cargo_temp_c is allowed to be empty (dry vans).
REQUIRED_FIELDS = [f for f in EVENT_FIELDS if f != "cargo_temp_c"]


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def rng_for(seed, *parts):
    """An independent random stream per purpose (e.g. per vehicle).

    String seeds are hashed with SHA-512 by Python, so results do not depend on
    PYTHONHASHSEED, on how vehicles are split across workers, or on run order.
    """
    return random.Random("-".join(str(p) for p in (seed,) + parts))


def seeded_uuid(rng):
    """uuid4-shaped ID drawn from a seeded stream (uuid.uuid4() is never reproducible)."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def poisson(rng, mean):
    """Small-mean Poisson draw (Knuth). Used for how many anomalies a trip gets."""
    limit, k, p = math.exp(-mean), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def parse_utc(text):
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def fmt_utc(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def bearing_deg(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def destination(lat, lon, bearing, dist_km):
    """Point reached by going dist_km from (lat, lon) on the given bearing."""
    d = dist_km / EARTH_RADIUS_KM
    b = math.radians(bearing)
    p1, l1 = math.radians(lat), math.radians(lon)
    p2 = math.asin(math.sin(p1) * math.cos(d) + math.cos(p1) * math.sin(d) * math.cos(b))
    l2 = l1 + math.atan2(math.sin(b) * math.sin(d) * math.cos(p1), math.cos(d) - math.sin(p1) * math.sin(p2))
    return math.degrees(p2), (math.degrees(l2) + 540) % 360 - 180


class Path:
    """A corridor polyline that can answer 'where am I after d km?'."""

    def __init__(self, points):
        self.points = points
        self.cum = [0.0]
        for (a_lat, a_lon), (b_lat, b_lon) in zip(points, points[1:]):
            self.cum.append(self.cum[-1] + haversine_km(a_lat, a_lon, b_lat, b_lon))
        self.length_km = self.cum[-1]

    def reversed(self):
        return Path(list(reversed(self.points)))

    def position_at(self, d_km):
        """Returns (lat, lon, bearing of the current segment)."""
        d_km = min(max(d_km, 0.0), self.length_km)
        i = min(bisect.bisect_right(self.cum, d_km) - 1, len(self.points) - 2)
        seg = self.cum[i + 1] - self.cum[i]
        t = (d_km - self.cum[i]) / seg if seg else 0.0
        (a_lat, a_lon), (b_lat, b_lon) = self.points[i], self.points[i + 1]
        return a_lat + (b_lat - a_lat) * t, a_lon + (b_lon - a_lon) * t, bearing_deg(a_lat, a_lon, b_lat, b_lon)


def write_csv(path, rows, fields):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))
