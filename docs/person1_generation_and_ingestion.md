# Person 1 - Data generation and ingestion

Owner: Person 1 · Status as of 2026-10-23 · Platform: Microsoft Fabric (workspace `Fleet_Telematics_Project`)

This document explains what was built for the first stage of the pipeline (generator -> bronze),
describes every dataset (metadata / data dictionary), lists what the other stages need from it,
and records what we noticed in the data that matters for the KPIs.

---

## 1. Status at a glance

| Work plan step | Status | Where |
|---|---|---|
| 1. Config file + seed | Done | `generator/config.yaml` |
| 2. Reference data (vehicles, drivers v1/v2, depots, corridors, trips) | Done, loaded into Fabric | `Files/reference`, tables `ref_*` |
| 3. Normal readings | Done | `generator/fleet/simulate.py` |
| 4. Three anomalies + `telemetry_truth` | Done (sample) | `Files/sample/telemetry_truth.csv`, table `telemetry_truth` |
| 5. Bad data on purpose (7 types) | Done (sample) | `Files/sample/bad_data_manifest.csv` |
| Day 1-2 handoff: ~1,000-event sample | Done | `Files/sample/sample_events.jsonl` |
| 7a. Streaming to Eventstream | Done | `fleet_eventstream` -> `bronze_telemetry` |
| 8. Bronze table (raw text + arrival time + source) | Done; **append-only proven from the Delta log and enforced** (`delta.appendOnly = true`) | `bronze_telemetry`, notebook `06_bronze_append_only_proof` |
| 10. Retry with backoff (1, 2, 4, 8 s) | Done and demonstrated live | `generator/fleet/stream_sender.py`, `fleet_run.py` |
| 6. Concurrent workers (5,000 vehicles) | Done: 16 async workers, 5,000 vehicles, 2 simulated h | `generator/run_fleet.py` |
| 7b. Batch landing (`Files/landing`) | Done: 62 files, 249,752 events loaded into bronze with `source = 'batch'` | `run_fleet.py --mode batch`, `upload_to_onelake.py`, notebook `05_load_landing` |
| 9. `dlq_telemetry` table | Done: 10,065 validation failures + 155 send failures, each with reason | notebook `04_dlq_and_ingestion_log` |
| 11. `bronze_ingestion_log` table | Done: sender runs, 10 s intervals and every validation pass | notebook `04_dlq_and_ingestion_log` |
| 12. Recovery demo | Done: 155 failed events replayed, DLQ status open -> recovered | `generator/replay_dlq.py` |
| 13. Speed measurement at full scale | Done: 2,026 events/s measured (732,019 events in 361 s) | `evidence/person1/full_run/perf.json` |

---

## 2. How it works

```mermaid
flowchart LR
    C[config.yaml + seed] --> G[Python generator]
    G -->|reference CSVs| REF[(Files/reference -> ref_* tables)]
    G -->|events.jsonl| S[send_stream.py]
    S -->|Event Hub protocol, envelope JSON| ES[fleet_eventstream<br/>custom endpoint]
    ES --> B[(bronze_telemetry)]
    S -.->|after 4 failed retries| DLQ[(DLQ file -> dlq_telemetry)]
    S -.-> LOG[(send log -> bronze_ingestion_log)]
    G -->|batch files .jsonl| U[upload_to_onelake.py] --> L[Files/landing] -->|notebook 05| B
    B --> P2[Person 2: silver]
```

**Run it** (from `generator/`):

```bash
python3 -m venv .venv && .venv/bin/pip install pyyaml azure-eventhub
.venv/bin/python generate.py all          # reference data + sample
.venv/bin/python send_stream.py           # send sample to Eventstream (needs .env)
```

Same `seed` => byte-identical output (checked by hashing two runs).

### 2.1 Fabric items

| Item | Type | Purpose |
|---|---|---|
| `fleet_lakehouse` | Lakehouse (schemas on, everything in `dbo`) | All files and Delta tables |
| `fleet_eventstream` | Eventstream | Source `generator_endpoint` (custom endpoint) -> destination Lakehouse table `bronze_telemetry` |
| `01_load_reference` | Notebook | Loads reference CSVs and the truth table into Delta tables |
| `02_check_bronze` | Notebook | Counts, distinct vehicles, malformed lines and table history of bronze |
| `03_check_full_run` | Notebook | Rows, distinct vehicles, arrival lag and partitions for one run |
| `04_dlq_and_ingestion_log` | Notebook | Validates new bronze rows, builds `dlq_telemetry` and `bronze_ingestion_log`, marks recovered events |
| `05_load_landing` | Notebook | Batch path: loads new files from `Files/landing` into bronze (`source = 'batch'`), archives them |
| `06_bronze_append_only_proof` | Notebook | Proves from the Delta transaction log that bronze was never modified, then sets `delta.appendOnly` |

### 2.2 Why every event is wrapped in an envelope

The Eventstream Lakehouse writer parses JSON. A malformed event (one of our deliberate bad-data
types) would be dropped silently, which breaks the "nothing is lost" rule. So the sender wraps
each event exactly as generated inside a small JSON object:

```json
{"raw": "<event text exactly as generated>", "source": "stream", "sent_at_utc": "...", "run_id": "..."}
```

Result: malformed lines reach bronze as text (proven: 5 of 5 malformed sample lines are in bronze),
and Person 2 parses `raw` with an explicit schema.

### 2.3 Generator behaviour (what "normal" looks like)

Values follow typical heavy-truck figures and are all set in `config.yaml`:

| Signal | Rule | Typical value |
|---|---|---|
| Reading interval | Every 30 s while the vehicle is on a trip | - |
| Speed | Cruise 82 km/h ± 6 (normal noise), ramps up over the first 4 readings | median 82 km/h |
| RPM | 700 idle + 12 x speed, capped at 1,900, ± 40 noise; 0 when engine off | median ~1,690 |
| Engine temp | Warms from 30 °C to 90 °C over 12 min, ± 1 noise; cools 0.4 °C per reading when off | median 88 °C |
| Fuel rate | 2.5 L/h idle + 0.33 x speed, ± 1 noise; 0 when engine off | median ~29.5 L/h |
| Odometer | Previous value + distance travelled since last reading | starts 20,000-400,000 km |
| Cargo temp | Reefers only: 5 °C ± 0.4; empty (null) for dry vans | 4-6 °C |
| Planned stop | Last 5 min of every 2 h of trip: speed 0, **rpm 0** (engine off) | not idling |

### 2.4 Anomalies (injected, recorded in `telemetry_truth`)

| Anomaly | How it is injected | Detection threshold (Section 0) | Injected range |
|---|---|---|---|
| Cold-chain breach | Reefer cargo temp rises to a 9-14 °C peak and back, every reading > 8 °C | outside 2-8 °C for ≥ 3 readings | 4-12 readings |
| Excessive idling | Vehicle holds position, speed 0, rpm ~700, fuel ~2.5 L/h | speed < 2 and rpm > 0 for ≥ 10 min | 12-30 min |
| Route deviation | GPS pushed sideways off the corridor | > 2 km off corridor for ≥ 3 readings | 2.5-6 km, 4-10 readings |

All injected values cross their thresholds with a margin (checked: breaches 9-11 °C,
deviations 2.8-5.0 km, idling 13-18 min in the sample). Anomalies never overlap each other or a planned stop.

### 2.5 Bad data (deliberate, recorded in `bad_data_manifest.csv`)

| Type | Example | Expected handling (Person 2) |
|---|---|---|
| `malformed_json` | JSON cut off mid-way | `silver_rejected` / DLQ |
| `missing_field` | `speed_kmh` key removed | reject (required field) |
| `wrong_type` | `"speed_kmh":"fast"` | reject (schema) |
| `null_required` | `"vehicle_id":null` | reject (required field) |
| `impossible_value` | `engine_temp_c: 900` | keep, flag as sensor error |
| `duplicate` | Same line sent twice (same `event_id`) | dedup on `vehicle_id + event_time` |
| `late_event` | Real `event_time`, delivered at the end of the stream | keep, place by `event_time` |

Bad data is never applied to anomaly readings, so every truth row points at a valid event.

---

## 3. Metadata

### 3.1 Dataset-level metadata

| Property | Value |
|---|---|
| Generator version | Commit in this repo (`generator/`) |
| Seed | 42 |
| Simulation start / window | 2026-11-01 06:00:00 UTC, 24 h |
| Time zone | All timestamps UTC, ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`) |
| Coordinates | WGS84 decimal degrees, 6 decimals (~0.1 m) |
| Units | km, km/h, rpm, °C, L/h (metric only) |
| Event format | JSON, one event per line (JSON Lines) |
| Fleet | 5,000 vehicles (1,500 refrigerated trucks, 3,500 dry vans), 5,200 drivers, 10 depots, 30 corridors, 10,000 trips |
| Geography | Depots in 10 Egyptian cities (Cairo, Alexandria, Port Said, Suez, Ismailia, Tanta, Mansoura, Minya, Asyut, Hurghada) |
| Sample | 20 vehicles x 50 readings = 1,000 events + 5 duplicate lines = 1,005 lines |
| Contains personal data | No. Driver names are random combinations of common first/last names |

### 3.2 Telemetry event (`sample_events.jsonl`, and `bronze_telemetry.raw`)

| Field | Type | Unit | Nullable | Description | Example |
|---|---|---|---|---|---|
| `event_id` | string (UUID) | - | No | Unique ID of the reading; seeded, so reproducible | `410bb39e-7287-...` |
| `vehicle_id` | string | - | No | `VEH-00001` ... `VEH-05000` | `VEH-00019` |
| `trip_id` | string | - | No | Trip the vehicle is on, see `ref_trips` | `TRP-000037` |
| `event_time` | string (ISO 8601 UTC) | - | No | When the reading was taken | `2026-11-01T06:02:00Z` |
| `latitude` | double | degrees | No | GPS latitude | `30.785576` |
| `longitude` | double | degrees | No | GPS longitude | `31.001689` |
| `speed_kmh` | double | km/h | No | Vehicle speed | `82.4` |
| `rpm` | integer | rpm | No | Engine speed; 0 = engine off | `1690` |
| `engine_temp_c` | double | °C | No | Engine temperature | `90.3` |
| `fuel_rate_lph` | double | L/h | No | Fuel consumption rate | `29.61` |
| `odometer_km` | double | km | No | Total distance so far (never decreases) | `399166.66` |
| `cargo_temp_c` | double | °C | **Yes** | Reefer cargo temperature; null for dry vans (normal) | `5.1` / `null` |

Business key for deduplication: `vehicle_id + event_time`.

### 3.3 `bronze_telemetry` (Delta, append-only and locked with `delta.appendOnly = true`)

| Column | Type | Source | Description |
|---|---|---|---|
| `raw` | string | sender | The event text exactly as generated (may be malformed on purpose) |
| `source` | string | sender | `stream` (later also `batch`) |
| `sent_at_utc` | string | sender | When the sender sent it |
| `run_id` | string | sender | One ID per sender run; links to the send log |
| `EventEnqueuedUtcTime` | timestamp | Eventstream | When Fabric received the event = **arrival time** |
| `EventProcessedUtcTime` | timestamp | Eventstream | When the Eventstream processed it |
| `PartitionId` | long | Eventstream | Eventstream partition (events are keyed by `vehicle_id`, so one vehicle stays in order) |

First load: 1,005 rows, 20 distinct vehicles, 5 lines that are not valid JSON (kept as text).

### 3.4 Reference tables (`Files/reference/*.csv` -> `ref_*`)

| Table | Rows | Key | Columns |
|---|---|---|---|
| `ref_depots` | 10 | `depot_id` | `name`, `lat`, `lon` |
| `ref_corridors` | 30 | `corridor_id` | `name`, `origin_depot_id`, `dest_depot_id`, `planned_distance_km`, `point_count`, `reefer_temp_min_c` (2), `reefer_temp_max_c` (8) |
| `ref_corridor_points` | 8,297 | `corridor_id + seq` | `latitude`, `longitude`, `km_from_start` (points ~1 km apart) |
| `ref_vehicles` | 5,000 | `vehicle_id` | `vehicle_type` (`refrigerated_truck` / `dry_van`), `make`, `model`, `model_year`, `home_depot_id`, `assigned_driver_id`, `start_odometer_km` |
| `ref_drivers_v1` | 5,200 | `driver_id` | `full_name`, `home_depot_id`, `risk_level` (low/medium/high), `license_class`, `hire_date`, `effective_from` |
| `ref_drivers_v2` | 5,200 | `driver_id` | Same columns; 416 drivers changed, `effective_from` = 2026-11-01 18:00 UTC |
| `ref_trips` | 10,000 | `trip_id` | `vehicle_id`, `driver_id`, `corridor_id`, `direction` (forward/reverse), `origin_depot_id`, `dest_depot_id`, `planned_distance_km`, `planned_start_utc`, `planned_end_utc` |

### 3.5 Operational tables (created by notebook 04)

| Table | Key | Columns |
|---|---|---|
| `dlq_telemetry` | `dlq_id` | `stage` (`send` / `validation`), `reason` (e.g. `missing_field: rpm`), `raw`, `source`, `run_id`, `arrived_at_utc`, `failed_at_utc`, `retries`, `status` (`open` / `recovered`), `recovered_at_utc`, `recovered_run_id`, `loaded_at_utc` |
| `bronze_ingestion_log` | `log_id` | `stage` (`sender_run` / `sender_interval` / `bronze_validation`), `run_id`, `source`, `interval_start_utc`, `duration_s`, `events_received`, `events_written`, `events_rejected`, `retries`, `active_vehicles`, `events_per_s`, `loaded_at_utc` |
| `landing_files_loaded` | `file_name` | `run_id`, `rows`, `file_written_utc`, `loaded_at_utc` (created by notebook 05; a file is never loaded twice) |
| `ingestion_watermark` | `job` | `last_enqueued_utc` (last bronze arrival time validated), `updated_at_utc` |

`bronze_telemetry.source` values: `stream` (live sender), `batch` (landing files), `dlq_replay` (recovered events).
All three are real telemetry; Person 2 should process all of them.

### 3.6 Evaluation and operational files

| File / table | Columns | Rule |
|---|---|---|
| `telemetry_truth` | `event_id`, `anomaly_type`, `anomaly_group_id` (one ID per injected episode, e.g. `IDL-VEH-00001-001`) | Evaluation only. **Detection code must never read it.** |
| `bad_data_manifest.csv` | `event_id`, `bad_type`, `detail` | Lets Person 2 check that every broken row was caught |
| `send_failures.jsonl` (local DLQ) | `raw`, `source`, `reason`, `failed_at_utc`, `retries`, `run_id` | Will be loaded into `dlq_telemetry` |
| `send_log.csv` (local log) | `run_id`, `source`, `started_utc`, `duration_s`, `events_read`, `events_sent`, `events_dlq`, `retries`, `events_per_s`, `dry_run` | Will be loaded into `bronze_ingestion_log` |

---

## 4. What we noticed that matters for the KPIs

Computed on the 1,005-event sample (clean, non-duplicate readings) and the reference data.

1. **Fuel efficiency baseline is 2.78 km/L (36 L/100 km)** - realistic for a heavy truck. Idling
   clearly hurts it: the two idling vehicles drop to 2.12 and 2.38 km/L vs a median of 2.82. So the
   KPI will react to the anomalies, which is good for the demo.
2. **Routes differ a lot in length**: 75 km (Ismailia - Port Said) to 579 km (Alexandria - Hurghada),
   median 274 km. Trips take 0.9 to 7.3 h. Long routes include planned stops, so per-route
   efficiency must use km and litres summed per route (non-additive ratio: divide at query time).
3. **Planned stops have rpm = 0, idling has rpm > 0.** Idle-minute calculations must keep the
   `rpm > 0` condition, otherwise every planned stop counts as idling.
4. **The "> 90 km/h" scorecard metric is mostly noise right now.** With cruise 82 ± 6 km/h, about
   9% of normal readings (89 of 975 in the sample) exceed 90 km/h, so every driver will have many.
   **Team decision needed:** lower `speed_noise_kmh` (e.g. to 3), or add a deliberate speeding
   behaviour for some drivers, or keep it and explain it.
5. **Cold-chain compliance in the sample is 95.2% (280 / 294)**, but only because the sample forces
   breaches. At the configured rate in a full run it will be above 99%. The denominator must only
   include refrigerated readings (dry vans have null cargo temperature by design).
6. **Driver history matters for the scorecard.** 416 drivers change in v2 (142 depot only, 128 risk
   only, 146 both); 398 of them drive a vehicle, and the change happens at 18:00 UTC, mid-simulation.
   Scorecards per depot/risk level will be wrong unless facts join to the driver version valid at
   `event_time` (SCD Type 2).
7. **Bad data would distort KPIs if it slipped through**: duplicates double litres, `engine_temp_c = 900`
   or `speed_kmh = 480` skew averages, `fuel_rate_lph = -5` makes efficiency jump. Recommend a
   gold quality check on value ranges.
8. Fleet is evenly spread: 457-542 vehicles per depot; every corridor gets 244-532 trips.

---

## 5. What orchestration (Person 4) needs from this stage

| Topic | Detail |
|---|---|
| What runs outside Airflow | The generator/sender and the Eventstream run continuously (work plan Step 3). Airflow does **not** start them. |
| Input to the DAG | Table `bronze_telemetry` (append-only: nothing downstream may delete or update it; Delta refuses it). New-data check: `max(EventEnqueuedUtcTime)` greater than the last processed watermark. |
| Incremental reads | Person 2 should keep a watermark on `EventEnqueuedUtcTime` (arrival time), not `event_time` (late events would be missed). |
| Batch path | `upload_to_onelake.py landing --watch` drops files into `Files/landing/` every 30 s. DAG order: notebook `05_load_landing` first, then `04_dlq_and_ingestion_log` (validates both stream and batch rows). |
| DLQ + ingestion log | Notebook `04_dlq_and_ingestion_log` is the first DAG task ("check for new bronze data" + validation). It is idempotent and incremental (watermark table `ingestion_watermark`). Its row counts feed the pipeline-health page. |
| Triggering notebooks | Fabric REST API: run a notebook on demand (`POST .../workspaces/{workspaceId}/items/{notebookId}/jobs/instances?jobType=RunNotebook`), or use Fabric's own Apache Airflow job item. Needs a service principal or a user token with access to the workspace. |
| Secrets | Eventstream connection string and the Fabric tenant ID live only in `generator/.env` (git-ignored). Never in the repo, notebooks or DAG code; use Airflow connections / Key Vault. OneLake uploads sign in interactively once (token cached in the macOS keychain). |
| Reference refresh | Reference tables are static per seed. Re-run `01_load_reference` only when `config.yaml` changes. |
| Evidence for the performance record | `send_log.csv` -> events/s per run (measured), machine noted separately. |

---

## 6. Evidence

| # | Screenshot | Shows |
|---|---|---|
| 1 | ![](../evidence/person1/01_load_reference_code.png) | Notebook `01_load_reference` attached to `fleet_lakehouse` |
| 2 | ![](../evidence/person1/02_load_reference_output.png) | 7 reference tables loaded, 5,000 vehicles / 1,500 reefers, 2 episodes of each anomaly |
| 3 | ![](../evidence/person1/03_eventstream_live_preview.png) | Eventstream live: `generator_endpoint` -> stream -> Lakehouse, raw events arriving |
| 4 | ![](../evidence/person1/04_check_bronze_code.png) | Notebook `02_check_bronze` |
| 5 | ![](../evidence/person1/05_check_bronze_output.png) | Bronze schema, 1,005 rows for the run, 20 vehicles, 5 malformed lines kept, table history |
| 6 | ![](../evidence/person1/06_check_full_run_code.png) | Notebook `03_check_full_run` for run `full-20261023T141747Z` |
| 7 | ![](../evidence/person1/07_check_full_run_output.png) | Full run in bronze: 732,019 rows (= events sent), **5,000 distinct vehicles**, 3.45 s average arrival lag, ~183k rows on each of the 4 partitions |
| 8 | ![](../evidence/person1/08_dlq_notebook_code.png) | Notebook `04_dlq_and_ingestion_log` |
| 9 | ![](../evidence/person1/09_dlq_before_replay_open.png) | **Before recovery**: 155 send failures `open`; 10,065 validation failures by reason type |
| 10 | ![](../evidence/person1/10_dlq_after_replay_recovered.png) | **After recovery**: the same 155 are `recovered` |
| 11 | ![](../evidence/person1/11_ingestion_log_intervals.png) | `bronze_ingestion_log`: validation pass, sender runs, 10 s intervals |
| 12 | ![](../evidence/person1/12_dlq_examples_and_ingestion_log.png) | Example DLQ rows with exact reasons (`missing_field: rpm`, `wrong_type: odometer_km is not a number`, ...) |
| 13 | ![](../evidence/person1/13_batch_validated_dlq.png) | Batch rows validated by notebook 04: 249,907 new rows, 3,414 to DLQ |
| 14 | ![](../evidence/person1/14_ingestion_log_with_batch.png) | Ingestion log with the batch sender run (249,752 lines in 62 files) and the `batch_load` row (249,752 rows into bronze) |
| 15 | ![](../evidence/person1/15_load_landing_output.png) | Notebook `05_load_landing`: 62 files, 249,752 rows appended as `batch`; bronze by source: stream 733,586, batch 249,752, dlq_replay 155 |
| 16 | ![](../evidence/person1/16_append_only_notebook_code.png) | Notebook `06_bronze_append_only_proof` reads the Delta transaction log |
| 17 | ![](../evidence/person1/17_append_only_commit_log.png) | Every commit of bronze: only files added, nothing removed (Eventstream commits have no operation name) |
| 18 | ![](../evidence/person1/18_append_only_checks_and_lock.png) | Check 1 PASS, lock set, DELETE and UPDATE refused, 983,493 rows intact (final run with Check 2 PASS: `evidence/person1/append_only/notebook06_final_output.txt`) |

Also in `evidence/person1/`: `config_used.yaml`, `sample_summary.json`, `send_log.csv`
(first real send: 1,005 events in 3.6 s = 278 events/s, 0 retries, 0 DLQ - measured on a MacBook,
sample only, one vehicle batch at a time).

---

## 7. Full run (5,000 vehicles)

Run `full-20261023T141747Z`, started 2026-10-23 14:17 UTC, measured on a MacBook (Apple Silicon, 10 cores, Python 3.13):

| Metric | Value |
|---|---|
| Vehicles | 5,000 (16 async workers, round-robin, one vehicle per worker and per partition) |
| Simulated time | 2 h (2026-11-01 06:00-08:00 UTC), every vehicle starts its first trip in this window |
| Readings generated | 724,887 |
| Lines sent (incl. 7,132 duplicates) | 732,019 |
| Retries / DLQ | 0 / 0 |
| Wall time | 361 s |
| **Throughput (measured)** | **2,026 events/s** (peak 10 s interval ~2,560 events/s) |
| Generator alone (dry run, measured) | ~65,000 events/s, so the network send is the bottleneck, not generation |
| **Confirmed in bronze (Fabric)** | 732,019 rows (= lines sent, nothing lost), **5,000 distinct vehicles** |
| Arrival lag (sender -> Eventstream, measured) | 3.45 s average; arrivals 14:17:53 to 14:23:49 UTC |
| Partition balance | 181,264 to 183,942 rows on each of the 4 partitions |
| Anomaly episodes | 48 cold-chain breaches, 96 excessive idling, 59 route deviations |
| Bad data | 2,282 malformed, 2,773 missing field, 2,145 wrong type, 2,841 null, 1,459 impossible, 7,132 duplicate, 2,180 late |

Per-run files in `evidence/person1/full_run/`: `perf.json`, `telemetry_truth.csv` (for Person 2's
evaluation of this run), `bad_data_manifest.csv`, `ingestion_intervals.csv` (one row per 10 s), console output.

Assumed, not measured: a full 24 h day is ~4 million events (~800 per vehicle, measured on 300 vehicles in a dry run),
which at the same rate would take ~33 min to send.

## 8. Dead-letter queue, ingestion log, retry and recovery

### 8.1 How failures are caught

| Where it fails | What happens | Ends up in |
|---|---|---|
| **Sending** (network / Eventstream unavailable) | Retry after 1, 2, 4, 8 s; after the 4th retry the batch is written to `output/dlq/send_failures.jsonl` with the reason | `dlq_telemetry`, `stage = 'send'` |
| **Reading** (event in bronze cannot be read or fails validation) | Notebook 04 checks each new bronze row: valid JSON, all required fields present and not null, numbers are numbers, `event_time` is ISO 8601 UTC. Bronze is never changed | `dlq_telemetry`, `stage = 'validation'` |

Impossible values (e.g. `engine_temp_c = 900`), duplicates and late events are **not** DLQ cases: they are
readable, so Person 2 keeps them (flag / deduplicate / place by `event_time`).

### 8.2 Retry demo (measured, live)

`python run_fleet.py --vehicles 20 --from-hours 2 --hours 2.25 --workers 1 --fail-first 6` sends **new** data
(simulated hours 2.00-2.25, not already in bronze) while simulating 6 failed send attempts:

```
send failed (simulated transient failure); retry 1/4 in 1s
send failed (simulated transient failure); retry 2/4 in 2s
send failed (simulated transient failure); retry 3/4 in 4s
send failed (simulated transient failure); retry 4/4 in 8s
gave up after 4 retries -> 155 events to DLQ
send failed (simulated transient failure); retry 1/4 in 1s     <- next batch recovers after one retry
```

Result: 545 generated + 4 duplicates = 549 lines = **394 sent + 155 to DLQ**, 5 retries (run `full-20261023T143702Z`).

### 8.3 Recovery demo

1. Notebook 04 loaded the 155 send failures: `send | ConnectionError | open | 155` (screenshot 9).
2. `python replay_dlq.py` resent them: 155 read, 155 sent, 0 failed (replay run `51cb41ef-...`, `source = 'dlq_replay'`).
3. Notebook 04 found the same raw events in bronze and marked them: `send | ConnectionError | recovered | 155` (screenshot 10).

### 8.4 Validation results on all bronze rows (733,586)

| Reason type | Rows in DLQ | Injected in full run + sample | Difference (from the two small test runs) |
|---|---|---|---|
| `malformed_json` | 2,288 | 2,282 + 5 = 2,287 | +1 (retry demo) |
| `missing_field` | 2,779 | 2,773 + 5 = 2,778 | +1 (smoke test) |
| `null_required` | 2,848 | 2,841 + 5 = 2,846 | +2 (retry demo) |
| `wrong_type` | 2,150 | 2,145 + 5 = 2,150 | 0 |
| **Total** | **10,065** | | 723,521 rows valid |

Every DLQ count is explained by what the generator injected: nothing broken slipped through and nothing valid
was rejected. (The retry demo's remaining bad events were among the 155 replayed rows, which the next
validation pass picks up.)

### 8.5 Running it

```bash
# on the sender machine, after any run:
#   upload generator/output/dlq/send_failures.jsonl, output/logs/send_log.csv,
#   output/logs/ingestion_intervals.csv to the Lakehouse folder Files/ops
python replay_dlq.py          # resend DLQ entries not replayed yet
```
Then run notebook `04_dlq_and_ingestion_log` (safe to re-run; it only checks bronze rows that arrived since
the last run and at least 2 minutes ago).

## 9. Batch path (Files/landing)

Both sending modes produce the same raw event lines; only the transport differs.

| Step | What | Measured |
|---|---|---|
| 1. Generate | `python run_fleet.py --mode batch --from-hours 2 --hours 2.5`: 16 workers write JSON-lines files (max 5,000 events each) to `output/landing/<run_id>/`, via a `.tmp` file + rename | 5,000 vehicles, 249,752 lines, **62 files in 11.6 s** (run `batch-20261023T150119Z`) |
| 2. Drop into landing | `python upload_to_onelake.py landing [--watch]` uploads new files to `Files/landing/<run_id>/` (upload as `.uploading`, rename when complete; 1-2-4-8 s retry) | 62 of 62 files uploaded |
| 3. Load | Notebook `05_load_landing` appends every line to `bronze_telemetry` with `source = 'batch'`, records the file in `landing_files_loaded`, moves it to `Files/archive/landing/` | **249,752 rows** into bronze (`batch_load` row in the ingestion log) |
| 4. Validate | Notebook `04` validates batch rows exactly like streamed rows | 249,907 rows checked (249,752 batch + 155 replayed), **3,414 to DLQ** |

Bronze now holds: `stream` 733,586 + `dlq_replay` 155 + `batch` 249,752 = **983,493 rows**.

DLQ reconciliation for this step: 787 malformed, 942 missing field, 934 null, 751 wrong type = 3,414 =
batch run injections (786 / 941 / 932 / 751) + the 5 broken events that were inside the 155 replayed rows.

**Time coverage.** Stream run: simulated 06:00-08:00; batch run: 08:00-08:30, so every vehicle continues
seamlessly. The 20 retry-demo vehicles also sent 08:00-08:15 by stream, so those readings arrive through
**both** paths with the same `event_id` - a deliberate cross-path duplicate case for Person 2's dedup.

The `ops` upload (`python upload_to_onelake.py ops`) also replaces the manual upload of the DLQ file and
sender logs to `Files/ops` from section 8.

## 10. Bronze is append-only (proof and enforcement)

`DESCRIBE HISTORY` cannot prove it: the Eventstream writer leaves `operation` empty (NULL). So notebook
`06_bronze_append_only_proof` reads the Delta **transaction log** (`_delta_log/*.json`), which records every
file added or removed by every commit, whoever wrote it.

| Check | Rule | Result (final run, 28 commits) |
|---|---|---|
| 1. No commit removed data | no `remove` action with `dataChange = true` (delete, update or overwrite would create one; compaction would not) | **0 -> PASS** |
| 2. Everything written is still there | rows added = rows now; the Eventstream writes no row statistics, so the notebook compares files instead: every file ever added is still part of the table | **38 of 38 files -> PASS** |
| Enforcement | `ALTER TABLE bronze_telemetry SET TBLPROPERTIES ('delta.appendOnly' = 'true')` | `DELETE ... WHERE 1 = 0` and `UPDATE ... WHERE 1 = 0` **refused** with `DELTA_CANNOT_MODIFY_APPEND_ONLY` |
| Writers still work | 30 events streamed after the lock (`full-20261024T011408Z`) | commits 24-27 after the lock; `stream` 733,586 -> 733,616; total **983,523 rows** |

The checks were also tested locally against a table where rows had been deleted: both checks fail there
(Check 1 counts the removed files, Check 2 finds files no longer in the table), so the proof can actually fail.
To lift the lock if ever needed: `ALTER TABLE bronze_telemetry UNSET TBLPROPERTIES ('delta.appendOnly')`.

## 11. Open items and next steps

All Person 1 steps of the work plan are done. Remaining items are team decisions and housekeeping:

1. **Team decision**: whether to also stream a full 24 h day (~4M events, ~33 min). The 2 h run already covers all 5,000 vehicles.
2. **Fabric trial ends in ~13 days (banner on 2026-10-24).** When it ends the workspace is deleted. Keep all code in GitHub (done), export evidence regularly, and agree with the workspace owner on a plan (another member's trial or a paid capacity) before then.
3. **Team decision** on the > 90 km/h noise (section 4, point 4).

---

## 12. Decisions and limitations

- Readings exist only while a vehicle is on a trip (telematics reports with ignition on).
- Each vehicle has its own random stream (`seed + vehicle_id`), so output does not depend on how vehicles are split across workers.
- Late events keep their real `event_time` and are delivered later in the stream.
- In the sample only, idling episodes are capped at ~20 min to fit inside 50 readings.
- Corridors are synthetic curves between real city coordinates, not real road geometry.
- The 30-second interval, behaviour rules and anomaly rates are our proposals; the handbook only asks for realistic, reproducible telemetry.
