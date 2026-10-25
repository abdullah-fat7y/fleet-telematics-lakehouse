
# Person 2 — Silver Processing, Anomaly Detection & Evaluation

## Fleet Telematics Lakehouse Project

**Branch:** `feat/silver_processing`  
**Primary technologies:** Microsoft Fabric, PySpark, Delta Lake, Spark SQL  
**Scope:** Bronze-to-Silver transformation, data quality validation, deduplication, movement feature engineering, anomaly detection, anomaly episode aggregation, and evaluation metrics.

---

# Table of Contents

1. [Project Overview](#1-project-overview)
2. [Bronze Layer — Raw Telemetry Inspection](#2-bronze-layer--raw-telemetry-inspection)
3. [Bronze-to-Silver Transformation](#3-bronze-to-silver-transformation)
4. [Data Validation and Rejected Records](#4-data-validation-and-rejected-records)
5. [Deduplication and Delta Lake Merge](#5-deduplication-and-delta-lake-merge)
6. [Vehicle Reference Enrichment](#6-vehicle-reference-enrichment)
7. [Window Functions and Haversine Distance](#7-window-functions-and-haversine-distance)
8. [Anomaly Detection](#8-anomaly-detection)
9. [Anomaly Episode Aggregation](#9-anomaly-episode-aggregation)
10. [Evaluation Against Ground Truth](#10-evaluation-against-ground-truth)
11. [Final Lakehouse Tables](#11-final-lakehouse-tables)
12. [Complete Execution Results](#12-complete-execution-results)
13. [Implementation Notes and Handover](#13-implementation-notes-and-handover)

---

# 1. Project Overview

## 1.1 Purpose

This document explains the implementation of Person 2 in the Fleet Telematics Lakehouse project.

The objective is to transform incoming vehicle telemetry into structured, validated, and deduplicated records, calculate vehicle movement features, detect abnormal readings, group related anomalies into episodes, and evaluate the detected episodes against available ground-truth data.

The documentation includes the implementation evidence and the observed Lakehouse query results so that the next developer can understand both the processing logic and the resulting data.

## 1.2 Processing Responsibilities

The implementation covers the following stages:

1. Inspect the raw Bronze telemetry table.
2. Parse raw JSON payloads using an explicitly defined PySpark schema.
3. Apply an ingestion-time watermark to limit incremental processing.
4. Validate mandatory telemetry fields.
5. Save rejected records separately with rejection metadata.
6. Remove duplicate vehicle readings.
7. Persist valid records to the Silver Delta table.
8. Enrich telemetry with vehicle reference attributes using a broadcast join.
9. Calculate time gaps, GPS distances, and derived speeds.
10. Identify idling and cold-chain temperature violations.
11. Detect five categories of anomalies.
12. Aggregate anomalous readings into time-based episodes.
13. Calculate precision, recall, and F1-score against ground-truth data.
14. Persist evaluation metrics in the Lakehouse.

## 1.3 High-Level Architecture

```text
                BRONZE LAYER
                      |
                      v
              bronze_telemetry
                      |
                      v
          JSON Parsing + Schema Enforcement
                      |
                      v
             Mandatory-Field Validation
                      |
              +-------+-------+
              |               |
              v               v
       silver_rejected     Valid Records
                              |
                              v
                         Deduplication
                              |
                              v
                       silver_telemetry
                              |
                              v
                   Vehicle Reference Join
                              |
                              v
                 Window and Movement Features
                              |
                              v
                      Anomaly Detection
                              |
                    +---------+---------+
                    |                   |
                    v                   v
        silver_telemetry_anomalies   Episode Grouping
                                        |
                                        v
                              silver_anomaly_episodes
                                        |
                                        v
                               Ground-Truth Evaluation
                                        |
                                        v
                                 evaluation_metrics
```

## 1.4 Technologies and Their Roles

| Technology | Role in this implementation |
|---|---|
| Microsoft Fabric | Notebook execution and Lakehouse environment |
| PySpark | Distributed data transformation and feature engineering |
| Spark SQL functions | JSON parsing, filtering, timestamp conversion, and calculations |
| Spark Window Functions | Previous-reading calculations and episode identification |
| Delta Lake | Persistent tables, incremental merge, and overwrite operations |
| SQL queries | Inspection and verification of Lakehouse outputs |

---

# 2. Bronze Layer — Raw Telemetry Inspection

## 2.1 Objective

Before transforming the telemetry data, the raw Bronze table is inspected to understand the incoming payload structure and ingestion metadata.

The Bronze layer retains the original JSON payload rather than storing every telemetry attribute as a separate column.

This provides a source layer that can be inspected independently of the downstream transformations.

## 2.2 Bronze Table

The source table is:

`bronze_telemetry`

The captured query selects the following fields:

- `raw`: original telemetry JSON payload.
- `source`: ingestion source.
- `sent_at_utc`: source-side sending timestamp.
- `run_id`: ingestion run identifier.
- `EventEnqueuedUtcTime`: event ingestion timestamp.
- `PartitionId`: source partition identifier.

## 2.3 Query Used

```sql
SELECT TOP (100)
    [raw],
    [source],
    [sent_at_utc],
    [run_id],
    [EventEnqueuedUtcTime],
    [PartitionId]
FROM [dbo].[bronze_telemetry];
```

## 2.4 Bronze Query Result

![Bronze Telemetry Raw Query Result](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/bronze_telemetry_raw_query.png)

### What the result demonstrates

1. Raw telemetry is available in JSON form.
2. Ingestion metadata accompanies the payload.
3. The ingestion timestamp can be used to identify newly received records.
4. The raw payload is the input for the Bronze-to-Silver parsing stage.

This is the starting point of the processing workflow.

---

# 3. Bronze-to-Silver Transformation

## 3.1 Objective

The purpose of this stage is to convert the raw JSON telemetry into a structured Spark DataFrame with explicitly defined field names and data types.

The resulting data is suitable for validation, deduplication, movement calculations, and anomaly detection.

## 3.2 Define the Telemetry Schema

The notebook defines a `StructType` schema for the raw telemetry payload.

| Field | Data type | Description |
|---|---|---|
| `event_id` | String | Telemetry event identifier |
| `vehicle_id` | String | Vehicle identifier |
| `trip_id` | String | Trip identifier |
| `event_time` | String, then Timestamp | Time at which the event occurred |
| `latitude` | Double | GPS latitude |
| `longitude` | Double | GPS longitude |
| `speed_kmh` | Double | Reported vehicle speed in km/h |
| `rpm` | Integer | Engine revolutions per minute |
| `engine_temp_c` | Double | Engine temperature |
| `fuel_rate_lph` | Double | Fuel consumption rate in liters per hour |
| `odometer_km` | Double | Odometer reading in kilometers |
| `cargo_temp_c` | Double | Cargo temperature |

The schema allows nullable fields so that missing values can be handled explicitly in the validation stage.

## 3.3 Incremental Processing

The notebook first checks whether the Silver table exists:

```python
silver_table_exists = spark.catalog.tableExists(
    "silver_telemetry"
)
```

If the table exists, the notebook retrieves the maximum previously processed `EventEnqueuedUtcTime`.

If the table does not exist, the watermark is initialized to `None`.

When a previous watermark exists, the Bronze DataFrame is filtered to include records whose ingestion timestamp is greater than the saved watermark.

This limits the input to newly ingested records under the configured watermark logic.

## 3.4 Parse the JSON Payload

The notebook uses `from_json()` to interpret the `raw` column using the predefined telemetry schema.

The parsed fields are expanded into individual DataFrame columns, and `event_time` is cast to `TimestampType`.

### Code — Parsing and Schema Enforcement

![Bronze to Silver Parsing Code](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/bronze_to_silver_parsing_code.png)

### Result — Structured Silver Telemetry

```sql
SELECT TOP (100)
    [EventEnqueuedUtcTime],
    [event_id],
    [vehicle_id],
    [trip_id],
    [event_time],
    [latitude],
    [longitude],
    [speed_kmh],
    [rpm],
    [engine_temp_c],
    [fuel_rate_lph],
    [odometer_km],
    [cargo_temp_c],
    [event_date]
FROM [dbo].[silver_telemetry];
```

![Silver Telemetry Query Result](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/silver_telemetry_query.png)

### Result interpretation

The Silver table contains individually accessible telemetry fields rather than only a raw JSON payload.

The data can now be queried by vehicle, trip, timestamp, coordinates, speed, engine metrics, fuel consumption, and cargo temperature.

The `event_date` column is derived from `event_time` and is used as the partition column when the table is initially created.

---

# 4. Data Validation and Rejected Records

## 4.1 Objective

Data validation prevents records with missing mandatory fields from entering the clean telemetry processing path.

Instead of discarding invalid records, the notebook preserves them in a separate Delta table with rejection metadata.

## 4.2 Mandatory-Field Validation

The validation condition requires all six fields to be non-null:

```python
validation_condition = (
    col("event_id").isNotNull() &
    col("vehicle_id").isNotNull() &
    col("event_time").isNotNull() &
    col("latitude").isNotNull() &
    col("longitude").isNotNull() &
    col("speed_kmh").isNotNull()
)
```

A record is considered valid only when every condition evaluates to true.

If any mandatory field is missing, the record is classified as rejected.

## 4.3 Rejection Metadata

The rejected DataFrame receives two additional columns:

- `rejection_reason`: describes the validation failure.
- `rejected_at`: records the current processing timestamp.

The configured rejection reason is:

`missing mandatory fields (null keys/coords/speed)`

The rejected records are written to `silver_rejected` in Delta format using append mode.

Valid records are retained in `df_valid` for subsequent processing.

## 4.4 Code — Validation Logic

![Validation Code and Counts](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/validation_code_and_counts.png)

## 4.5 Result — Validation Counts

The captured execution produced the following results:

| Metric | Count |
|---|---:|
| Total parsed records | 985,323 |
| Total rejected records | 8,725 |
| Total valid records | 974,798 |

![Validation Results](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/validation_results.png)

The counts reconcile:

`985,323 = 8,725 + 974,798`

Therefore, the captured validation output accounts for all parsed records.

## 4.6 Inspect the Rejected Table

```sql
SELECT TOP (100)
    [EventEnqueuedUtcTime],
    [event_id],
    [vehicle_id],
    [trip_id],
    [event_time],
    [latitude],
    [longitude],
    [speed_kmh],
    [rpm],
    [engine_temp_c],
    [fuel_rate_lph],
    [odometer_km],
    [cargo_temp_c],
    [rejection_reason],
    [rejected_at]
FROM [dbo].[silver_rejected];
```

![Silver Rejected Query Result](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/silver_rejected_query.png)

### What the result demonstrates

The rejected table retains the parsed telemetry values, including null fields, and adds information about why and when the records were rejected.

This supports data-quality investigations without losing the original parsed record.

### Validation limitations

The supplied implementation checks whether mandatory fields are null. It does not independently validate GPS coordinate ranges, negative speeds, or other business-specific constraints.

---

# 5. Deduplication and Delta Lake Merge

## 5.1 Objective

Remove duplicate readings from the valid dataset before persisting the cleaned records to the Silver table.

The implementation defines a duplicate using the combination of `vehicle_id` and `event_time`.

## 5.2 Deduplication Logic

```python
df_deduplicated = df_valid.dropDuplicates(
    ["vehicle_id", "event_time"]
)

valid_count = df_valid.count()
deduped_count = df_deduplicated.count()

duplicates_removed = valid_count - deduped_count
```

This retains one row for each vehicle/timestamp combination within the current valid DataFrame.

If duplicate rows have different non-key values, `dropDuplicates()` does not guarantee which row will be retained.

## 5.3 Code — Deduplication and Merge

![Deduplication and Merge Code](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/deduplication_and_merge_code.png)

## 5.4 Result — Deduplication Counts

| Metric | Count |
|---|---:|
| Valid records before deduplication | 974,798 |
| Duplicates removed | 11,243 |
| Clean deduplicated records | 963,555 |

![Deduplication Results](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/deduplication_results.png)

The counts reconcile:

`974,798 - 11,243 = 963,555`

## 5.5 Derive the Event Date

The clean DataFrame receives an `event_date` column derived from `event_time`:

```python
df_silver_clean = df_deduplicated.withColumn(
    "event_date",
    to_date("event_time")
)
```

## 5.6 Persist the Silver Table

When `silver_telemetry` already exists, the notebook performs a Delta Lake merge using the following matching condition:

```sql
target.vehicle_id = source.vehicle_id
AND target.event_time = source.event_time
```

Only source rows that do not match an existing target row are inserted.

When the table does not exist, the notebook creates the Delta table and partitions it by `event_date`.

### Why the merge is important

- It avoids inserting already-matching vehicle/timestamp combinations.
- It supports incremental processing.
- It preserves the existing table when no new unmatched records are available.
- Initial date partitioning supports date-oriented data access.

### Implementation limitation

The merge inserts unmatched rows but does not update rows that already match the target key.

The watermark, deduplication, and merge logic should be reviewed together when handling late-arriving events or repeated ingestion runs.

---

# 6. Vehicle Reference Enrichment

## 6.1 Objective

Enrich telemetry records with vehicle reference attributes before calculating operational features.

The notebook attempts to read `ref_vehicles` and join it to `silver_telemetry` using `vehicle_id`.

## 6.2 Broadcast Join

```python
df_ref_vehicle = spark.read.table("ref_vehicles")

df_enriched = df_silver.join(
    F.broadcast(df_ref_vehicle),
    on="vehicle_id",
    how="left"
)
```

A broadcast join distributes the smaller reference DataFrame to the Spark worker nodes. When the reference table is sufficiently small, this can reduce the shuffle required by a conventional join.

The join uses `left`, so telemetry rows remain present even when no matching reference record exists.

If reading the reference table or performing the join raises an exception, the notebook falls back to `df_silver`.

## 6.3 Code — Reference Enrichment

![Broadcast Join Code](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/broadcast_join_code.png)

### Result interpretation

When the join succeeds, the enriched DataFrame contains the telemetry data and the matching vehicle reference attributes, such as vehicle type, make, model, model year, depot, or assigned driver when those columns are available in the reference table.

The screenshot documents the join implementation; it does not independently quantify the performance improvement.

---

# 7. Window Functions and Haversine Distance

## 7.1 Objective

Calculate movement-related features by comparing each vehicle's current reading with its previous reading.

The calculations use Spark Window Functions, GPS coordinates, and timestamps.

## 7.2 Define the Window

```python
window_spec = Window.partitionBy(
    "vehicle_id"
).orderBy("event_time")
```

Partitioning by `vehicle_id` ensures that each vehicle's readings are processed independently. Ordering by `event_time` establishes the sequence of readings.

The notebook uses `lag()` to derive:

- `prev_latitude`
- `prev_longitude`
- `prev_event_time`

## 7.3 Calculate the Time Gap

The difference between the current and previous event timestamps is calculated in seconds:

```python
df_windowed = df_windowed.withColumn(
    "time_gap_sec",
    F.unix_timestamp("event_time")
    - F.unix_timestamp("prev_event_time")
)
```

This value is used as the elapsed time for the derived speed calculation.

## 7.4 Calculate Distance Using the Haversine Formula

The Haversine formula estimates the great-circle distance between two GPS coordinates on a spherical Earth.

The implementation uses an Earth radius of approximately 6,371 km.

The output column is `distance_km`.

If the previous latitude is unavailable, the distance defaults to `0.0`.

## 7.5 Calculate GPS-Derived Speed

The notebook calculates:

```python
calculated_speed_kmh = distance_km / (
    time_gap_sec / 3600.0
)
```

The division is performed only when `time_gap_sec` is non-null and greater than zero.

Otherwise, `calculated_speed_kmh` defaults to `0.0`.

This feature provides an independent speed estimate based on consecutive GPS readings.

## 7.6 Detect Idling

The implementation sets `is_idling` to true when:

- `speed_kmh < 2`
- `rpm > 0`

This identifies readings where reported vehicle speed is very low while the engine is running.

## 7.7 Detect Cold-Chain Temperature Violations

The implementation sets `is_cold_chain_violation` to true when `cargo_temp_c` is available and either:

- `cargo_temp_c < 2`, or
- `cargo_temp_c > 8`.

The configured acceptable interval is therefore 2°C through 8°C, inclusive.

## 7.8 Code — Window Features and Haversine Distance

![Window Haversine and Detection Features Code](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/window_haversine_detection_features.png)

### Output feature summary

| Feature | Meaning |
|---|---|
| `prev_latitude` | Previous latitude for the same vehicle |
| `prev_longitude` | Previous longitude for the same vehicle |
| `prev_event_time` | Previous event timestamp |
| `time_gap_sec` | Elapsed seconds between readings |
| `distance_km` | Haversine distance between consecutive readings |
| `calculated_speed_kmh` | GPS-derived speed in km/h |
| `is_idling` | Indicator for idling conditions |
| `is_cold_chain_violation` | Indicator for cargo temperatures outside the configured interval |

These features form the basis of the anomaly detection stage.

---

# 8. Anomaly Detection

## 8.1 Objective

Identify abnormal vehicle readings using explicit operational thresholds.

The notebook generates individual Boolean flags, combines them into an overall anomaly indicator, and assigns an anomaly category.

## 8.2 Detection Rules

| Anomaly | Condition |
|---|---|
| GPS teleportation | `calculated_speed_kmh > 200` |
| Engine overheating | `engine_temp_c > 105` |
| RPM spike | `rpm > 4500` |
| Cold-chain breach | `is_cold_chain_violation` is true |
| Excessive idling | `is_idling` is true |

These are the thresholds implemented in the notebook. They should be treated as configured project rules rather than universal industry limits.

## 8.3 Combine Anomaly Flags

The notebook creates the following individual flags:

- `is_gps_teleportation`
- `is_engine_overheating`
- `is_rpm_spike`
- `is_cold_chain_breach`
- `is_excessive_idling`

It then calculates `is_anomaly` using the logical OR of all five flags.

A reading is marked anomalous if at least one of the configured conditions is true.

## 8.4 Assign the Anomaly Type

The notebook assigns `anomaly_type` using conditional expressions in this order:

1. `cold_chain_breach`
2. `excessive_idling`
3. `route_deviation`
4. `engine_overheating`
5. `rpm_spike`

If multiple anomaly flags are true for one reading, the first matching condition determines the stored category. The individual flags remain available separately.

## 8.5 Persist the Anomaly Table

The resulting DataFrame is saved to:

`silver_telemetry_anomalies`

The write uses Delta format, overwrite mode, and schema overwrite enabled.

### Code — Anomaly Detection and Episode Logic

![Anomaly Detection and Episode Code](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/anomaly_detection_and_episode_code.png)

## 8.6 Query the Anomaly Table

```sql
SELECT TOP (100)
    [vehicle_id],
    [EventEnqueuedUtcTime],
    [event_id],
    [trip_id],
    [event_time],
    [latitude],
    [longitude],
    [speed_kmh],
    [rpm],
    [engine_temp_c],
    [fuel_rate_lph],
    [odometer_km],
    [cargo_temp_c],
    [event_date],
    [vehicle_type],
    [make],
    [model],
    [model_year],
    [home_depot_id],
    [assigned_driver_id],
    [start_odometer_km],
    [prev_latitude],
    [prev_longitude],
    [prev_event_time],
    [time_gap_sec],
    [distance_km],
    [calculated_speed_kmh],
    [is_idling],
    [is_cold_chain_violation],
    [is_gps_teleportation],
    [is_engine_overheating],
    [is_rpm_spike],
    [is_cold_chain_breach],
    [is_excessive_idling],
    [is_anomaly],
    [anomaly_type]
FROM [dbo].[silver_telemetry_anomalies];
```

### Result — Anomaly Table

![Silver Telemetry Anomalies Query Result](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/silver_telemetry_anomalies_query.png)

The query result displays telemetry records alongside engineered movement features, vehicle reference attributes when available, anomaly flags, and the assigned anomaly category.

The captured screenshot shows a sample of the table rather than a complete count of anomalous readings.

---

# 9. Anomaly Episode Aggregation

## 9.1 Objective

Group related anomalous readings into episodes so that consecutive detections can be analyzed as a continuous operational event.

The implementation partitions the data by vehicle and anomaly type, orders it by event time, and identifies the start of each new episode.

## 9.2 Identify Consecutive Anomalies

The episode window uses:

- `vehicle_id`
- `anomaly_type`
- `event_time`

The notebook retrieves the previous anomaly timestamp using `lag()` and calculates the gap in minutes.

## 9.3 Start a New Episode

The implementation starts a new episode when:

- There is no previous anomaly timestamp, or
- The gap between consecutive anomalous readings exceeds five minutes.

Otherwise, the reading remains in the same episode sequence.

A cumulative sum of `new_episode_flag` produces the `episode_id`.

## 9.4 Aggregate Episodes

The DataFrame is grouped by:

- `vehicle_id`
- `anomaly_type`
- `episode_id`

The following values are calculated:

| Output column | Calculation |
|---|---|
| `start_time` | Minimum event timestamp in the episode |
| `end_time` | Maximum event timestamp in the episode |
| `reading_count` | Number of readings in the episode |
| `max_calculated_speed` | Maximum calculated speed in the episode |
| `duration_seconds` | Difference between the end and start timestamps |

The output is written to `silver_anomaly_episodes` in Delta format using overwrite mode with schema overwrite enabled.

## 9.5 Query the Episode Table

```sql
SELECT TOP (100)
    [vehicle_id],
    [anomaly_type],
    [episode_id],
    [start_time],
    [end_time],
    [reading_count],
    [max_calculated_speed],
    [duration_seconds]
FROM [dbo].[silver_anomaly_episodes];
```

### Result — Aggregated Anomaly Episodes

![Silver Anomaly Episode Query Result](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/silver_anomaly_episode_query.png)

### Interpretation

Each result row represents a grouped episode for a vehicle and anomaly category. The table records when the episode started and ended, how many readings it contains, and the maximum calculated speed observed during that episode.

The screenshot provides a sample of the resulting episode table.

### Episode-grouping note

The implementation groups anomalies by vehicle and anomaly type, splitting episodes when the gap exceeds five minutes. It does not explicitly match the episode against ground-truth time intervals at this stage.

---

# 10. Evaluation Against Ground Truth

## 10.1 Objective

Evaluate the detected anomaly episodes against the available ground-truth table.

The notebook attempts to read:

- `telemetry_truth`
- `silver_anomaly_episodes`

If evaluation succeeds, the calculated metrics are stored in `evaluation_metrics`.

## 10.2 Extract Vehicle IDs from Ground Truth

The implementation extracts a vehicle identifier from `anomaly_group_id` using:

```python
F.regexp_extract(
    F.col("anomaly_group_id"),
    r"(VEH-\d+)",
    1
)
```

The extracted value is stored as `extracted_vehicle_id`.

## 10.3 Match Detected Episodes to Ground Truth

The notebook performs an inner join using:

1. Equality between the detected vehicle ID and the extracted ground-truth vehicle ID.
2. A case-insensitive comparison between the detected and ground-truth anomaly types.

It selects distinct combinations of detected `vehicle_id` and `episode_id` and counts these as true positives.

## 10.4 Evaluation Metrics

### Precision

Precision measures the proportion of detected episodes counted as true positives.

```text
Precision = True Positives / Total Detected Episodes
```

### Recall

Recall measures the proportion of ground-truth groups counted as matched.

```text
Recall = True Positives / Total Ground-Truth Groups
```

### F1-score

The F1-score combines precision and recall:

```text
F1-score = 2 × Precision × Recall
           / (Precision + Recall)
```

The implementation returns zero if the relevant denominator is zero.

## 10.5 Code — Evaluation Logic

![Evaluation Code](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/evaluation_code.png)

## 10.6 Query the Persisted Metrics

```sql
SELECT
    [precision],
    [recall],
    [f1_score],
    [true_positives],
    [total_detected],
    [total_truth]
FROM [dbo].[evaluation_metrics];
```

### Result — Evaluation Metrics

![Evaluation Metrics Query Result](https://raw.githubusercontent.com/abdullah-fat7y/fleet-telematics-lakehouse/feat/silver_processing/evidence/person2/evaluation_metrics_queryy.png)

## 10.7 Recorded Evaluation Results

| Metric | Value |
|---|---:|
| True positives | 2 |
| Total detected episodes | 1,201 |
| Total ground-truth groups | 6 |
| Precision | 0.0017 (0.17%) |
| Recall | 0.3333 (33.33%) |
| F1 Score | 0.0033 |

### Interpretation

- One detected episode was counted as a true positive.
- The implementation counted 1,201 detected episodes.
- Six distinct ground-truth groups were counted.
- The recorded recall is approximately 16.67%.
- The recorded precision and F1-score are very low.

These values are the recorded outputs of the supplied implementation. They should not be interpreted as a definitive measure of detection quality until the matching methodology has been validated.

### Important Evaluation Limitation

The join matches by vehicle ID and anomaly type but does not compare episode time intervals.

The implementation also counts distinct detected episodes as true positives. Consequently, a detected episode may match a ground-truth group without proving that the two represent the same temporal event.

A more rigorous evaluation should define a one-to-one matching rule and, where appropriate, compare anomaly time ranges.

---

# 11. Final Lakehouse Tables

The following tables are referenced in the supplied processing workflow.

| Table | Purpose | Write/read behavior |
|---|---|---|
| `bronze_telemetry` | Raw telemetry source | Read |
| `silver_telemetry` | Structured and deduplicated telemetry | Incremental merge if existing; initial Delta write otherwise |
| `silver_rejected` | Records failing mandatory-field validation | Append |
| `ref_vehicles` | Vehicle reference attributes | Read for enrichment |
| `silver_telemetry_anomalies` | Telemetry, engineered features, and anomaly flags | Overwrite |
| `silver_anomaly_episodes` | Aggregated anomaly episodes | Overwrite |
| `telemetry_truth` | Ground-truth anomaly groups | Read |
| `evaluation_metrics` | Persisted evaluation metrics and counts | Overwrite |

The physical availability and schema of these tables should be verified in the target Lakehouse before rerunning the notebooks.

---

# 12. Complete Execution Results

## 12.1 Validation Results

| Metric | Value |
|---|---:|
| Total parsed records | 985,323 |
| Total rejected records | 8,725 |
| Total valid records | 974,798 |

The validation counts reconcile:

`985,323 = 8,725 + 974,798`

## 12.2 Deduplication Results

| Metric | Value |
|---|---:|
| Valid records before deduplication | 974,798 |
| Duplicates removed | 11,243 |
| Clean deduplicated records | 963,555 |

The deduplication counts reconcile:

`974,798 - 11,243 = 963,555`

## 12.3 Evaluation Results

| Metric | Value |
|---|---:|
| True positives | 2 |
| Total detected episodes | 1,201 |
| Total ground-truth groups | 6 |
| Precision | 0.0017 (0.17%) |
| Recall | 0.3333 (33.33%) |
| F1 Score | 0.0033 |

## 12.4 Complete Screenshot Inventory

The following screenshots are referenced in this document.

| Screenshot filename | Evidence |
|---|---|
| `bronze_telemetry_raw_query.png` | Raw Bronze telemetry query |
| `bronze_to_silver_parsing_code.png` | JSON parsing and schema enforcement |
| `silver_telemetry_query.png` | Structured Silver table query |
| `validation_code_and_counts.png` | Validation implementation |
| `validation_results.png` | Validation counts |
| `silver_rejected_query.png` | Rejected records query |
| `deduplication_and_merge_code.png` | Deduplication and merge implementation |
| `deduplication_results.png` | Deduplication counts |
| `broadcast_join_code.png` | Vehicle reference broadcast join |
| `window_haversine_detection_features.png` | Movement feature engineering |
| `anomaly_detection_and_episode_code.png` | Anomaly detection and episode code |
| `silver_telemetry_anomalies_query.png` | Anomaly table query |
| `silver_anomaly_episode_query.png` | Aggregated episode query |
| `evaluation_code.png` | Evaluation implementation |
| `evaluation_metrics_queryy.png` | Persisted evaluation metrics query |

---

# 13. Implementation Notes and Handover

The following points should be reviewed by the next developer before extending or rerunning the pipeline.

1. **Source schema:** Confirm that the JSON structure in `bronze_telemetry` still matches `telemetry_schema`.
2. **Incremental watermark:** Review how `EventEnqueuedUtcTime` is used to select new records, especially when events arrive late or out of order.
3. **Rejected records:** Confirm whether the append behavior is appropriate for repeated runs and whether rejected events need their own deduplication strategy.
4. **Validation rules:** The supplied validation checks mandatory null fields only; add domain checks when required.
5. **Deduplication key:** Confirm that `(vehicle_id, event_time)` uniquely identifies a reading for the project's business rules.
6. **Deterministic deduplication:** Define a tie-breaking rule if duplicate records can contain conflicting values.
7. **Delta merge:** The existing-table merge inserts unmatched records and does not update matched rows.
8. **Reference join:** Check whether `ref_vehicles` contains duplicate `vehicle_id` values that could multiply telemetry rows.
9. **Broadcast join:** Verify that the reference table is small enough to broadcast safely.
10. **GPS calculations:** Validate the Haversine calculation and derived speed against known sample coordinates and timestamps.
11. **First reading:** The first reading for a vehicle has no previous coordinates; the implementation defaults its distance and derived speed to zero.
12. **Non-positive time gaps:** The speed calculation defaults to zero when the time gap is null or non-positive.
13. **Idling threshold:** The implementation uses reported speed below 2 km/h and RPM above zero.
14. **Cold-chain thresholds:** The implementation flags cargo temperature below 2°C or above 8°C.
15. **Anomaly thresholds:** GPS-derived speed above 200 km/h, engine temperature above 105°C, and RPM above 4,500 trigger their respective flags.
16. **Anomaly precedence:** When multiple anomaly flags are true, the first matching condition determines `anomaly_type`.
17. **Episode threshold:** A new episode starts when the gap between consecutive anomalous readings exceeds five minutes.
18. **Evaluation matching:** The current evaluation joins vehicle ID and anomaly type without matching episode time ranges.
19. **Evaluation counts:** The recorded metrics should be recalculated after any changes to the anomaly detection or matching logic.
20. **Table write modes:** `silver_rejected` uses append mode; the anomaly and episode tables and evaluation metrics use overwrite mode.
21. **Screenshot paths:** All evidence links use the `feat/silver_processing` branch and the `evidence/person2/` directory.
22. **Image filenames:** The evaluation screenshot is referenced as `evaluation_metrics_queryy.png`, matching the filename in the working link supplied for this documentation.

---

# Conclusion

Person 2's implementation covers the Silver-layer transformation and quality-control workflow, movement feature engineering, anomaly detection, episode aggregation, and evaluation against ground-truth data.

The captured results show:

- 985,323 parsed records.
- 8,725 rejected records.
- 974,798 valid records before deduplication.
- 11,243 duplicates removed.
- 963,555 clean deduplicated records.
- 1,201 detected anomaly episodes in the recorded evaluation.
- One true-positive episode counted by the implemented matching logic.

The document preserves the implementation details and recorded outputs while identifying areas that should be reviewed before the evaluation metrics are used as a definitive measure of anomaly detection quality.
