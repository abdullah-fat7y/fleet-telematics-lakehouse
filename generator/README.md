# Person 1 - Fleet generator (days 1-2)

## Run it

```bash
pip install pyyaml
python generate.py all
```

Same `seed` in `config.yaml` => byte-identical files every time (checked: two runs give the same hash).

## What it produces

`output/Files/reference/` (the fleet description, made once):

| File | Rows | What it is |
|---|---|---|
| depots.csv | 10 | Depots in real Egyptian cities (editable in config) |
| corridors.csv | 30 | Fixed routes between depots, planned km, allowed reefer range 2-8 C |
| corridor_points.csv | ~8,300 | GPS points ~1 km apart along each corridor |
| vehicles.csv | 5,000 | 1,500 refrigerated trucks, 3,500 dry vans, home depot, driver, start odometer |
| drivers_v1.csv | 5,200 | Drivers with depot and risk level (200 spares) |
| drivers_v2.csv | 5,200 | 416 drivers changed depot/risk, `effective_from` = mid-simulation (for Person 3's SCD Type 2) |
| trips.csv | 10,000 | Out-and-back trip per vehicle: corridor, direction, planned start/end |

`output/Files/sample/` (the handoff for Person 2):

| File | What it is |
|---|---|
| sample_events.jsonl | 1,005 raw lines: 20 vehicles x 50 readings, plus duplicates. One JSON event per line, exactly the Section 0 fields |
| telemetry_truth.csv | `event_id, anomaly_type, anomaly_group_id` for every injected anomaly reading. Evaluation only - detection must never read it |
| bad_data_manifest.csv | Which events were deliberately broken and how. Lets Person 2 check their rejects |
| sample_summary.json | Counts of everything above |

The sample contains 2 episodes each of cold-chain breach, excessive idling and route deviation,
and 5 each of malformed JSON, missing field, wrong type, null required field, duplicate,
impossible value and late event.

## Design decisions (for the decisions list)

- Readings are produced only while a vehicle is on a trip (telematics reports with ignition on).
  Planned stops (5 min every 2 h) still report, with rpm 0 - so they are *not* idling.
- Each vehicle has its own random stream (seed + vehicle_id), so the output does not change
  when vehicles are split across a different number of workers.
- Bad data is never applied to anomaly readings, so the truth table always points at valid events.
- Late events keep their real `event_time` and are delivered later in the stream (in the sample: at the end of the file).
- In the sample only, idling episodes are capped at ~20 min so they fit inside 50 readings.

## Where things go

| Thing | Where |
|---|---|
| Code (`generate.py`, `fleet/`, `config.yaml`) | Team GitHub repo, `generator/` folder |
| Reference CSVs | Fabric Lakehouse `Files/reference`, then Delta tables via `ingestion/notebooks/01_load_reference.py` |
| Sample files | Fabric Lakehouse `Files/sample` |
| Evidence (config, summary, screenshots) | GitHub `evidence/` + a Drive backup (Fabric trial deletes the workspace when it ends) |
