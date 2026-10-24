"""Deliberate bad data: turns a clean event into the raw text that actually gets sent.

This is separate from the anomalies. Anomalies are valid readings that describe a
problem on the road; bad data is broken input that the pipeline must catch.
"""
import json

from .common import REQUIRED_FIELDS

MALFORMED = "malformed_json"
MISSING_FIELD = "missing_field"
WRONG_TYPE = "wrong_type"
NULL_REQUIRED = "null_required"
DUPLICATE = "duplicate"
IMPOSSIBLE = "impossible_value"
LATE = "late_event"
BAD_TYPES = [MALFORMED, MISSING_FIELD, WRONG_TYPE, NULL_REQUIRED, DUPLICATE, IMPOSSIBLE, LATE]

WRONG_TYPE_VALUES = {
    "speed_kmh": ["fast", "N/A", "82,5"],
    "rpm": ["idle", "1.7k"],
    "engine_temp_c": ["hot", "--"],
    "fuel_rate_lph": ["unknown"],
    "latitude": ["thirty", "30°02'N"],
    "longitude": ["east"],
    "odometer_km": ["n/a"],
    "event_time": ["yesterday", "01/11/2026 06:00"],
}
IMPOSSIBLE_VALUES = [
    ("engine_temp_c", 900.0), ("speed_kmh", 480.0), ("rpm", -300), ("fuel_rate_lph", -5.0),
    ("latitude", 123.4), ("longitude", -250.0),
]


def to_raw(event):
    return json.dumps(event, separators=(",", ":"))


def pick_bad_type(cfg, rng):
    """Rate-based choice for full runs. Returns None for a clean event."""
    r, acc = rng.random(), 0.0
    for kind in BAD_TYPES:
        acc += cfg["bad_data"][f"{kind}_rate"]
        if r < acc:
            return kind
    return None


def corrupt(rng, event, kind):
    """Returns (list of raw strings to send, detail). Late events are sent unchanged but delayed."""
    e = dict(event)
    if kind == MALFORMED:
        raw = to_raw(e)
        return [raw[: rng.randint(10, len(raw) - 5)]], "truncated JSON"
    if kind == MISSING_FIELD:
        field = rng.choice(REQUIRED_FIELDS)
        del e[field]
        return [to_raw(e)], f"removed {field}"
    if kind == WRONG_TYPE:
        field = rng.choice(sorted(WRONG_TYPE_VALUES))
        e[field] = rng.choice(WRONG_TYPE_VALUES[field])
        return [to_raw(e)], f"{field} sent as text"
    if kind == NULL_REQUIRED:
        field = rng.choice(REQUIRED_FIELDS)
        e[field] = None
        return [to_raw(e)], f"{field} is null"
    if kind == IMPOSSIBLE:
        field, value = rng.choice(IMPOSSIBLE_VALUES)
        e[field] = value
        return [to_raw(e)], f"{field}={value}"
    if kind == DUPLICATE:
        raw = to_raw(e)
        return [raw, raw], "same event sent twice"
    if kind == LATE:
        return [to_raw(e)], "delivered late"
    raise ValueError(kind)
