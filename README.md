# Enterprise Fleet Telematics Lakehouse & Dynamic Logistics Warehouse

<img width="736" height="736" alt="502c02a4e4e54f388aafd6410626eb16" src="https://github.com/user-attachments/assets/969883ed-232e-4dbf-bd6e-645b4f4c12f9" />


A SIC 8 / 2026 Big Data Engineering graduation project that turns simulated fleet telemetry into auditable operational analytics using a hybrid source system, Microsoft Fabric, Delta Lake, and Power BI.

> **Data notice:** All vehicle, driver, trip, telemetry, and anomaly data in this project is simulated. Dashboard figures describe this simulation and its implemented rules; they are not evidence of real-world fleet incidents or regulatory compliance.

## Contents

- [1. Business problem and objectives](#1-business-problem-and-objectives)
- [2. End-to-end architecture](#2-end-to-end-architecture)
- [3. Source data and generation](#3-source-data-and-generation)
- [4. Pipeline lifecycle](#4-pipeline-lifecycle)
- [5. Gold analytical model](#5-gold-analytical-model)
- [6. Dashboard and business questions](#6-dashboard-and-business-questions)
- [7. Setup and execution](#7-setup-and-execution)
- [8. Validation and measured evidence](#8-validation-and-measured-evidence)
- [9. Limitations and items to verify](#9-limitations-and-items-to-verify)
- [10. Repository guide and detailed documentation](#10-repository-guide-and-detailed-documentation)
- [11. Team responsibilities](#11-team-responsibilities)

## 1. Business problem and objectives

Fleet operators need to understand vehicle efficiency, driver behaviour, refrigerated-cargo conditions, route deviations, and unusual engine readings. These signals arrive as time-stamped telemetry and are difficult to compare when they are processed independently. Millions of raw readings are also too detailed for managers to inspect directly.

This project simulates a delivery fleet and builds a data platform that preserves incoming events, validates and enriches them, detects rule-based anomalies, creates analytical tables, and presents results in a Power BI dashboard.

### Intended users

- **Fleet and operations managers** — compare routes, fuel efficiency, driver behaviour, cold-chain readings, and detected anomaly episodes.
- **Data engineers** — inspect ingestion, rejected records, quality checks, reconciliation, and job durations.
- **Project reviewers** — follow the pipeline end to end and assess whether outputs are reproducible and supported by evidence.

### Objectives

1. Generate repeatable, domain-specific telemetry and controlled data-quality issues.
2. Support both continuous ingestion and scheduled historical/batch ingestion.
3. Preserve source records and rejection reasons for audit and recovery.
4. Produce cleaned, deduplicated Silver telemetry and anomaly episodes.
5. Publish a Gold dimensional model and KPI tables for Power BI.
6. Validate outputs with data-quality checks and Silver-to-Gold reconciliation.
7. Document measured results, assumptions, and limitations rather than imply production readiness.

## 2. End-to-end architecture

The implemented v2 design is hybrid: the simulated fleet and operational source services run locally in Docker Compose, while Microsoft Fabric hosts the cloud ingestion endpoint, OneLake landing area, lakehouse tables, and analytics workflow.

### Architecture diagram

<img width="1280" height="592" alt="Final Architecture" src="https://github.com/user-attachments/assets/6d6cce05-08c5-4b90-92a4-4c5c0735cf18" />

<!-- Add the complete project architecture image here. Keep this Mermaid diagram as the readable, version-controlled architecture. -->

### Components and why they are used

| Component | Responsibility | Design reason |
|---|---|---|
| Python generator | Creates fleet reference data, telemetry, anomalies, and malformed events | Reproducible test data without relying on real vehicle data |
| FastAPI | Exposes `POST /v1/telemetry?channel=live\|history` | Provides a single source-system entry point and records validation outcomes |
| PostgreSQL | Stores `telemetry_live`, `telemetry_history`, and ingestion/extraction logs | Represents the operational database before analytical processing |
| Debezium CDC + Kafka | Publishes new live-table rows to `fleet.public.telemetry_live` | Separates source writes from downstream streaming delivery |
| Bridge | Consumes Kafka and sends events to Fabric Eventstream | Commits offsets after cloud acknowledgement and retries transient failures |
| Hourly extractor | Exports history in hourly slices to OneLake | Supports incremental loads, retries, and backfills |
| Microsoft Fabric Eventstream | Receives the live Kafka path | Managed cloud entry point for streaming telemetry |
| OneLake | Holds historical landing files and lakehouse data | Shared storage for the batch and analytics paths |
| Delta Lake / Fabric lakehouse | Stores Bronze, Silver, and Gold tables | Supports table-based processing and analytical workloads |
| PySpark / Spark SQL | Parses, validates, deduplicates, enriches, detects, and aggregates | Makes transformations explicit and suitable for large event volumes |
| Power BI | Presents KPIs, route/driver analysis, anomaly investigation, and pipeline health | Makes curated outputs accessible to operational users and reviewers |

The project uses the Medallion pattern: **Bronze preserves ingested records, Silver produces curated telemetry and detection outputs, and Gold serves business-oriented tables.** Apache Airflow is part of the graduation handbook's recommended framework, but the supplied v2 documentation describes a Fabric REST notebook launcher and execution workflow; do not assume an Airflow DAG is implemented unless it is present in the repository.

## 3. Source data and generation

### Simulated fleet

The generator describes:

- 5,000 vehicles: 1,500 refrigerated trucks and 3,500 dry vans.
- 5,200 drivers, 10 depots, 30 corridors, and 10,000 trips in the pilot reference data.
- Depots across Cairo, Alexandria, Port Said, Suez, Ismailia, Tanta, Mansoura, Minya, Asyut, and Hurghada.
- A nominal telemetry interval of 30 seconds while a vehicle is on a trip.
- UTC timestamps in ISO 8601 format and WGS84 latitude/longitude coordinates.
- Metric units: kilometres, km/h, rpm, °C, and litres/hour.

The pilot generator uses seed `42`; its documentation states that identical seed/configuration runs produce byte-identical generated output. The v2 source-system documentation describes four simulated days of trips (40,000) and separates the original pilot day from the later batch and streaming runs.

### Telemetry event schema

The event payload is JSON Lines: one JSON event per line. The documented telemetry fields are:

| Field | Meaning | Notes |
|---|---|---|
| `event_id` | Unique reading identifier | UUID string |
| `vehicle_id` | Vehicle identifier | `VEH-00001` through `VEH-05000` |
| `trip_id` | Associated trip | References trip data |
| `event_time` | Time the reading was taken | ISO 8601 UTC string |
| `latitude`, `longitude` | GPS position | Decimal degrees |
| `speed_kmh` | Vehicle speed | km/h |
| `rpm` | Engine speed | `0` represents engine off |
| `engine_temp_c` | Engine temperature | °C |
| `fuel_rate_lph` | Fuel consumption rate | litres/hour |
| `odometer_km` | Cumulative odometer | km; expected not to decrease |
| `cargo_temp_c` | Cargo temperature | °C; null is normal for dry vans |

The v2 Bronze table adds source and ingestion metadata, including `source` (`kafka_cdc` or `db_extract`), `sent_at_utc`, `run_id`, `db_id`, `is_valid`, `reject_reason`, `db_received_at`, Kafka partition/offset, and Fabric event-arrival/processing fields. `db_id` is the downstream deduplication key for v2 because streaming delivery is at-least-once.

### Injected anomalies and data-quality cases

| Case | Simulation or example | Intended treatment |
|---|---|---|
| Cold-chain breach | Refrigerated cargo temperature rises above the 2–8 °C range | Detect a sustained out-of-range episode |
| Excessive idling | Vehicle remains nearly stationary with engine running | Detect a sustained idle period |
| Route deviation | GPS position is displaced from the planned corridor | Flag position readings that exceed the configured distance/time rule |
| Engine overheating | Engine temperature is driven to an abnormal range | Detect using a rule-based threshold |
| Malformed JSON | Truncated JSON text | Preserve raw event and record a rejection |
| Missing field / null required field | Required field omitted or null | Reject from curated telemetry, retain reason |
| Wrong type | For example, `"speed_kmh":"fast"` | Reject during schema/type validation |
| Impossible sensor value | For example, `engine_temp_c: 900` | Keep visible for audit/quality analysis where the implemented rules allow it |
| Duplicate | Repeated event | Prevent duplicate analytical records using the configured identifier/business key |
| Late event | Event time is earlier than its arrival time | Retain and order analytically by event time where supported |

The pilot generator's documented business deduplication key is `vehicle_id + event_time`. The v2 Bronze stream uses `db_id` to identify the source database row. These keys serve different purposes and should not be treated as interchangeable.

`telemetry_truth` and `bad_data_manifest.csv` provide simulation truth and bad-data metadata for evaluation. Ground-truth labels are for evaluation and should not be used by detection rules to reveal the expected answer.

## 4. Pipeline lifecycle

### 4.1 Generate and ingest

The generator produces reference data and telemetry. In v2, the FastAPI endpoint stores each submitted line in PostgreSQL along with `is_valid` and `reject_reason`, and records request-level ingestion information. Live inserts travel through Debezium and Kafka; history is extracted into hourly gzip JSON Lines files.

### 4.2 Bronze — preserve source evidence

- Pilot: `bronze_telemetry`, populated through Fabric Eventstream and the `Files/landing` batch path.
- v2: `bronze_telemetry_v2`, populated by Eventstream for Kafka CDC and notebook `11_load_landing_v2` for history extracts.
- The pilot documents append-only Delta configuration (`delta.appendOnly = true`), a dead-letter table, and ingestion logs.
- The v2 bridge uses retries and a dead-letter file; the batch loader inserts each `db_id` once and archives loaded files.

The pilot and v2 Bronze tables are separate generations of the ingestion design; they should not be described as one table.

### 4.3 Silver — validate, enrich, and detect

The Silver workflow documented in `person2.md` reads `bronze_telemetry_v2`, parses raw JSON using an explicit schema, validates required fields, preserves rejected records, deduplicates, partitions telemetry by event date, enriches records with reference data, calculates movement metrics, and applies anomaly rules.

Documented Silver outputs include:

- `silver_telemetry`
- `silver_rejected`
- `silver_telemetry_anomalies`
- `silver_anomaly_episodes`
- `evaluation_metrics`

The exact contents of these tables depend on the executed pipeline and its run scope. The Silver implementation document also records known limitations in cross-micro-batch deduplication, retries, speed filtering, and episode-level evaluation.

### 4.4 Gold — model data for business questions

Gold notebooks `NB01` to `NB04` are documented as building dimensions, facts, KPI tables, quality results, reconciliation results, and job timings. The Gold layer stores ratio numerators and denominators so Power BI can calculate ratios after filtering, rather than incorrectly adding or averaging row-level percentages.

### 4.5 Orchestration, monitoring, and reporting

The v2 documentation describes `platform/fabric/run_notebook.py` as a REST API launcher that starts a Fabric notebook and waits for completion. Monitoring evidence includes ingestion/extraction logs, rejected records, quality results, Silver-to-Gold reconciliation, and job durations.

Power BI imports Gold tables into a semantic model and exposes six pages: Fleet Overview, Routes and Cold Chain, Driver Scorecard, Anomaly Investigation, Pipeline Health, and Definitions & Methodology.

## 5. Gold analytical model

The documented Gold tables and their analytical grains are listed below. The grain is important: it tells consumers what one row represents and helps avoid double counting.

| Table | Grain | Analytical purpose |
|---|---|---|
| `gold_dim_date` | One day | Date filtering and trends |
| `gold_dim_time` | One minute | Time-of-day and day-part analysis |
| `gold_dim_vehicle` | One vehicle | Vehicle type and vehicle context |
| `gold_dim_driver` | One driver version (SCD Type 2) | Driver name, depot, risk level, and historical changes |
| `gold_dim_route` | One corridor/route | Route names and route comparisons |
| `gold_dim_anomaly_type` | One anomaly type plus “none” | Anomaly labels and classification |
| `gold_fact_telemetry` | One 30-second reading, partitioned by date | Reading-level telemetry measures |
| `gold_fact_trip` | One trip | Trip duration, idle minutes, idle share, refrigerated readings |
| `gold_fact_anomaly_episode` | One detected episode | Episode count, duration, time of day, and investigation details |
| `gold_kpi_fuel_efficiency_route` | Route × day | Distance, fuel used, and fuel-efficiency numerator/denominator |
| `gold_kpi_cold_chain` | Route × day | In-range readings, total readings, compliance, breach episodes |
| `gold_kpi_driver_scorecard` | Driver version × day | Idling, excessive-idling episodes, speeding readings, route deviations |
| `gold_quality_results` | One quality check per run | Quality status and failing-row counts |
| `gold_reconciliation_results` | One KPI metric per run | Silver-to-Gold comparison and match status |
| `gold_job_run_times` | One job run | Job execution durations |

The supplied model-view documentation notes that `gold_fact_telemetry` exists in Gold but was not visible in the supplied Power BI model screenshot. It also notes that the three operational tables (`gold_quality_results`, `gold_reconciliation_results`, `gold_job_run_times`) are standalone log-style tables rather than being related to the other model tables.

### Metric definitions

| Metric | Documented rule |
|---|---|
| Fuel efficiency | `SUM(total_km) / SUM(total_litres)`; exclude intervals longer than 300 seconds and GPS jumps |
| Cold-chain compliance | Refrigerated readings between 2 and 8 °C inclusive divided by all refrigerated readings |
| Cold-chain breach episode | Three or more consecutive readings outside 2–8 °C, counted as one episode |
| Speeding | Speed strictly above 90 km/h |
| Idling | Speed below 2 km/h with RPM above zero; planned stops with RPM zero are excluded |
| Excessive idling | Idling stretch of 10 minutes or more |
| Route deviation | More than 2 km from corridor for at least three readings |
| Episodes per 1,000 km | Episode count divided by distance, multiplied by 1,000 |
| Reconciliation | Gold KPI values compared with values recalculated from Silver, using relative tolerance `1e-6` |

Cold-chain compliance is a simulated threshold metric, not a regulatory certification. Anomaly episodes are rule detections, not independently verified real-world incidents.

## 6. Dashboard and business questions

| Page | Main question | Main data objects |
|---|---|---|
| **1. Fleet Overview** | How is the fleet performing overall, and which anomaly types dominate? | Fuel-efficiency and cold-chain KPI tables; anomaly episodes; route dimension |
| **2. Routes and Cold Chain** | Which routes differ in fuel efficiency or refrigerated-cargo conditions? | `gold_kpi_fuel_efficiency_route`, `gold_kpi_cold_chain`, `gold_dim_route`, `gold_dim_date` |
| **3. Driver Scorecard** | Where are idling, speeding, and route deviations concentrated? | `gold_kpi_driver_scorecard`, `gold_dim_driver`, `gold_fact_trip` |
| **4. Anomaly Investigation** | What happened, where, when, and for how long? | `gold_fact_anomaly_episode`, vehicle/route/date/time/anomaly dimensions |
| **5. Pipeline Health** | Did quality checks run, do Gold values reconcile with Silver, and how long did jobs take? | `gold_quality_results`, `gold_reconciliation_results`, `gold_job_run_times` |
| **6. Definitions & Methodology** | What rules, thresholds, and limitations define the metrics? | Text-only methodology page |

### Reported snapshot figures

The dashboard documentation records the following figures for its documented report state. They should be treated as a snapshot of that run, not guaranteed results for every rerun.

| Measure | Reported value |
|---|---:|
| Fleet fuel efficiency | 2.77 km/L |
| Cold-chain compliance | 99.86% |
| Cold-chain breach episodes | 163 |
| Detected anomaly episodes | 9,724 |
| Vehicles reporting | 5,000 target |
| Quality checks | 28 PASS, 1 WARN, 0 FAIL |
| Reconciliation | 100.00% match across 52,917 groups compared |
| Drivers with SCD Type 2 history | 416 |

The dashboard documentation also flags interpretation issues: route-deviation episodes appear to be influenced by GPS position jumps; route efficiency differences rounded to two decimals display as `0.00`; a daily-volume visual needs its measure confirmed; and the time-of-day chart's relationship to the time dimension needs verification. Review these before using the report as a final operational decision tool.

## 7. Setup and execution

### Prerequisites

The documented v2 path requires:

- Git and Docker Compose.
- Python and the generator's Python dependencies.
- Access to a Microsoft Fabric workspace with the required lakehouse, Eventstream, and notebooks.
- Fabric REST/API configuration for notebook execution.
- A Fabric Eventstream v2 Kafka connection string.
- Permission to upload to the relevant OneLake landing area.

The supplied documents do not establish a single, fully tested clean-machine setup covering every dependency version and every Fabric resource. Use the commands below as the documented execution path and confirm the current repository configuration before running a full load.

### Configuration and secrets

The v2 documentation places `EVENTSTREAM_V2_CONNECTION_STRING`, `FABRIC_TENANT_ID`, and `FABRIC_WORKSPACE_ID` in `generator/.env`. The pilot also uses an environment file for Eventstream connectivity.

1. Create the local environment file expected by the code.
2. Add the required values from your own Fabric workspace and Eventstream.
3. Keep `.env` files, access tokens, passwords, and connection strings out of version control.
4. Do not paste credentials into notebooks, logs, screenshots, or this README.
5. Confirm `.gitignore` excludes local secrets before committing.

Do not copy real secrets into a shared team document.

### Documented v2 execution path

The following commands are recorded in `person1_v2_source_system.md`. They are shown in the documented Linux/macOS-style shell syntax; Windows users should use a compatible shell or adapt the virtual-environment executable path. The commands assume the documented directory structure and preconfigured Fabric resources.

Start the local source system and register the CDC connector:

```bash
cd platform
docker compose up -d --build
./connect/register.sh
```

Start the bridge profile (requires `EVENTSTREAM_V2_CONNECTION_STRING` in `generator/.env`):

```bash
docker compose --profile cloud up -d --build bridge
```

Run a historical batch, extract the history slice, and load it to Fabric:

```bash
cd ../generator
.venv/bin/python run_fleet.py --mode api --channel history --from-hours 24 --hours 48

cd ../platform/extractor
../../generator/.venv/bin/python extract.py --date 2026-11-02

cd ../fabric
../../generator/.venv/bin/python run_notebook.py 11_load_landing_v2
```

Run the documented streaming golden-run command:

```bash
cd ../../generator
.venv/bin/python run_fleet.py --mode api --channel live --from-hours 72 --hours 96 --rate 700 --minutes 75
```

This streaming command is the documented 75-minute run, not a quick smoke test. It can generate millions of records and consume Fabric capacity. Verify the active simulated date, target workspace, permissions, and capacity before executing it.

The pilot has a separate generation/ingestion path documented in [`person1_generation_and_ingestion.md`](person1_generation_and_ingestion.md). Do not substitute pilot commands for v2 commands without confirming which Bronze table and ingestion endpoint the downstream notebooks read.

## 8. Validation and measured evidence

### v2 batch ingestion

The source-system report records five batch scenarios: initial load/backfill, incremental load, a rerun demonstrating idempotency, failure/retry, and historical backfill.

| Scenario | Rows read | Inserted | Skipped |
|---|---:|---:|---:|
| Initial load / backfill | 4,014,197 | 4,014,197 | 0 |
| Incremental #1 | 293,897 | 293,897 | 0 |
| Incremental #2 with repeated hour | 573,047 | 279,150 | 293,897 |
| Failure + retry | 502,818 | 502,818 | 0 |
| Backfill of an already loaded hour | 424,918 | 0 | 424,918 |
| **Total inserted from history extracts** | — | **5,090,062** | — |

The documented run retained 69,971 rejected rows with reasons. An initial upload timeout was recovered by retrying unfinished hourly extracts after changing uploads to 4 MB chunks.

### v2 streaming golden run

| Measurement | Documented result |
|---|---:|
| Wall-clock duration | 75.2 minutes |
| Generation rate | 700 events/second |
| Events in the run | 3,157,802 |
| Distinct `db_id` in the run's Bronze result | 3,157,802 |
| Database-to-cloud latency | Average 1.59 s; p50 1.58 s; p95 2.36 s; max 7.34 s |
| Kafka lag | Median 690 messages; max 1,427 |
| API-rejected records | 43,340 |
| Injected late events | 9,353 |

The bridge was restarted during the run. The source-system report says the cloud and Kafka totals showed no lost or duplicated golden-run records across that restart. Its interval log had a gap of about 5,000 deliveries around the restart; the report says a shutdown-summary logging fix was made afterward.

### Cross-hop reconciliation

The source-system documentation reports **8,260,189 distinct source rows** across the v2 history and streaming data: 5,090,062 batch rows and 3,170,127 stream rows. The streaming count includes the run's separately noted total in Bronze, while the golden-run result table reports 3,157,802 distinct IDs for the golden run itself; this distinction reflects the broader accumulated table versus the individual run.

The dashboard documentation reports 8,146,714 valid Silver readings and 113,475 rejected rows, which sum to 8,260,189. These figures reconcile arithmetically with the documented v2 total, but should still be rechecked against live table counts before a new demonstration.

### Gold quality and reconciliation

The documented Power BI snapshot shows 28 PASS, 1 WARN, and no FAIL; 100.00% reconciliation across 52,917 groups; and 416 driver histories represented with SCD Type 2. The warning for out-of-range sensor values is described as intentional: the pipeline keeps those values visible rather than silently deleting them.

The Gold documentation records job durations such as `NB02 TOTAL` at 263 seconds, `NB02 fact_telemetry` at 114 seconds, `NB04 reconciliation` at 103 seconds, and `NB03 quality_checks` at 83 seconds. These are run-specific measurements, not general performance guarantees.

### Anomaly evaluation caveat

The supplied Silver document reports one evaluation result of precision `1.0000`, recall `0.8333`, and F1 `0.9091` (5 true positives, 5 detections in scope, 6 truth episodes). The dashboard documentation separately refers to a Gold README evaluation of precision `0.167`, recall `0.333`, and F1 `0.222`. These results appear to use different scopes or evaluation logic. Do not combine or present one as a correction of the other until the exact evaluation code, run, and unit of matching (reading vs episode) are verified.

### Evidence locations

The source-system documentation names these evidence locations:

- `evidence/person1/v2_batch_runs/` — batch run summaries, console logs, and extraction-log evidence.
- `evidence/person1/v2_stream_golden_run/` — streaming golden-run evidence.
- `evidence/person1/full_run/perf.json` — pilot full-run performance record.

The Silver documentation also references screenshots under `evidence/person2/`. Check that these paths exist in the checked-out branch before linking them from a report.

## 9. Limitations and items to verify

### Known limitations

- The dataset is synthetic; anomaly patterns and dashboard values are only as realistic as the generator and rules.
- The local source stack uses one laptop and a single Kafka broker with replication factor 1. This is suitable for a project demonstration, not high availability.
- The Fabric trial/capacity is limited, and an open interactive Spark session has previously blocked a loader run with `TooManyRequestsForCapacity`.
- The source-system report warned that the trial workspace and its cloud tables could expire; code and evidence should be retained independently.
- The Silver document identifies potential duplicate handling gaps across micro-batches and between `db_id` and the business key `vehicle_id + event_time`.
- A failure between separate rejected/valid writes may lead to a partial micro-batch result on retry unless writes are made idempotent.
- The Silver document flags a speed-filtering order issue that can hide extreme calculated speeds from a later anomaly rule.
- Episode-level anomaly evaluation requires clearly documented matching criteria.
- Some dashboard visuals and model relationships are explicitly flagged for review in the dashboard documentation.

### Checks before presenting

- Confirm whether the report is showing pilot data, v2 data, or a mixture of runs.
- Verify the exact current row counts in Bronze, Silver, Gold, and rejected-record tables.
- Re-run quality checks and Silver-to-Gold reconciliation for the run being demonstrated.
- Filter the Pipeline Health page to the intended run where necessary.
- Confirm the `gold_fact_anomaly_episode` to `gold_dim_time` relationship used by the time-of-day chart.
- Confirm the definition behind the Daily Data Volume visual.
- Display route-efficiency differences with enough decimal places to make small differences visible.
- Reconcile the two documented anomaly-evaluation result sets before claiming a single model-quality score.
- Verify that the complete architecture image, dashboard screenshots, and evidence directories are present at the paths linked by the final repository.

### Future improvements

- Add automated tests for schema contracts, business-key uniqueness, idempotent retries, and late-event handling.
- Use episode-level truth matching based on vehicle, anomaly type, and temporal overlap.
- Improve monitoring for source-to-cloud latency, backlog, rejected events, and retry outcomes.
- Make orchestration dependencies and run parameters explicit and reproducible.
- Review the dimensional model relationships and dashboard measures against the documented grain of each table.
- Add a clean-environment deployment guide and a repeatable end-to-end smoke test.
- Consider higher-availability broker configuration and managed secrets for a production deployment.

## 10. Repository guide and detailed documentation

The paths below are referenced in the supplied project documents. Confirm them against the branch being released, because the live repository tree was not available for direct inspection when this README was prepared.

| Path / resource | Purpose |
|---|---|
| `generator/` | Pilot generator, configuration, sender, and fleet run commands |
| `platform/api/` | FastAPI ingestion API |
| `platform/postgres/` | PostgreSQL source-system setup |
| `platform/bridge/` | Kafka-to-Fabric streaming bridge |
| `platform/extractor/` | Hourly history extraction |
| `platform/fabric/` | Fabric notebook REST launcher |
| `generator/config.yaml` | Pilot generator configuration and seed |
| `person1_v2_source_system.md` | v2 API, PostgreSQL, CDC/Kafka, batch/stream runs, reconciliation, and commands |
| `person1_generation_and_ingestion.md` | Pilot generation, Eventstream ingestion, Bronze, DLQ, metadata, and evidence |
| `person2.md` | Silver parsing, validation, deduplication, enrichment, anomaly rules, episode aggregation, and evaluation |
| `evidence/person1/v2_batch_runs/` | Batch evidence referenced by the v2 document |
| `evidence/person1/v2_stream_golden_run/` | Streaming golden-run evidence |
| `evidence/person1/full_run/perf.json` | Pilot throughput evidence |

The project handbook is the source for graduation-project requirements and acceptance expectations. The Fleet Analytics Dashboard documentation describes the business context, six report pages, Gold model, metric definitions, and presentation checks. Include these documents in the repository only if they are actually committed there; do not assume uploaded reference files are already repository files.

## 11. Team responsibilities

Fill in the actual names and final contributions before submission. The rows below are intentionally blank so the team can record verified ownership without attributing work by assumption.

| Team member | Role / area |
|---|---|
| Shahesta Mohamed | Source generation and ingestion |
| Ahmed Mohamed | Silver processing and anomaly detection |
| Yahya Moustafa | Gold model |
| Abdullah Fathy | BI Development |

---

**Project:** SIC 8 — Big Data Engineering Track, 2026  
**Domain:** Supply Chain & Intelligent Logistics — Enterprise Fleet Telematics Lakehouse & Dynamic Logistics Warehouse  
**Core platform:** Microsoft Fabric, Delta Lake, PySpark, Power BI
