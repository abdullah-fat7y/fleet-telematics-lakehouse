
# Fleet Telematics Lakehouse
## Person 2 — Silver Processing, Telemetry Analytics, Anomaly Detection, and Real-Time Fleet Alerts

---

## Table of Contents

- [1. Project Overview](#1-project-overview)
- [2. Scope of Work](#2-scope-of-work)
- [3. Technology Stack](#3-technology-stack)
- [4. Processing Architecture](#4-processing-architecture)
- [5. Bronze Layer Inspection](#5-bronze-layer-inspection)
- [6. Bronze-to-Silver Processing](#6-bronze-to-silver-processing)
- [7. Schema Parsing](#7-schema-parsing)
- [8. Data Validation](#8-data-validation)
- [9. Rejected Records Handling](#9-rejected-records-handling)
- [10. Deduplication](#10-deduplication)
- [11. Incremental Processing and Duplicate Prevention](#11-incremental-processing-and-duplicate-prevention)
- [12. Silver Table Partitioning](#12-silver-table-partitioning)
- [13. Telemetry Enrichment](#13-telemetry-enrichment)
- [14. Vehicle Movement Analytics](#14-vehicle-movement-analytics)
- [15. Distance and Speed Calculation](#15-distance-and-speed-calculation)
- [16. Idling and Cold-Chain Detection](#16-idling-and-cold-chain-detection)
- [17. Anomaly Detection Rules](#17-anomaly-detection-rules)
- [18. Anomaly Type Assignment](#18-anomaly-type-assignment)
- [19. Anomaly Episode Aggregation](#19-anomaly-episode-aggregation)
- [20. Incremental Silver Streaming Pipeline](#20-incremental-silver-streaming-pipeline)
- [21. Real-Time Fleet Alerts Pipeline](#21-real-time-fleet-alerts-pipeline)
- [22. Silver Telemetry SQL Verification](#22-silver-telemetry-sql-verification)
- [23. Rejected Records SQL Verification](#23-rejected-records-sql-verification)
- [24. Anomaly Records SQL Verification](#24-anomaly-records-sql-verification)
- [25. Anomaly Episodes SQL Verification](#25-anomaly-episodes-sql-verification)
- [26. Real-Time Alerts SQL Verification](#26-real-time-alerts-sql-verification)
- [27. Anomaly Evaluation](#27-anomaly-evaluation)
- [28. Evaluation Results](#28-evaluation-results)
- [29. Data Quality Considerations](#29-data-quality-considerations)
- [30. Limitations and Improvement Opportunities](#30-limitations-and-improvement-opportunities)
- [31. Final Summary](#31-final-summary)

---

# 1. Project Overview

The Fleet Telematics Lakehouse project processes vehicle telemetry data to support vehicle monitoring, movement analytics, cold-chain monitoring, anomaly detection, and real-time operational alerting.

The system receives telemetry records containing vehicle identifiers, trip identifiers, timestamps, GPS coordinates, speed, engine measurements, fuel information, odometer readings, and cargo temperature.

The data is processed through a Medallion Architecture consisting of Bronze, Silver, and Gold layers.

The Bronze layer stores ingested data. The Silver layer applies data quality rules, transformations, deduplication, enrichment, and analytical calculations. Additional processing pipelines identify operational anomalies and generate real-time alerts from selected incoming telemetry records.

The project contains two distinct monitoring approaches:

1. **Analytical anomaly detection:** Processes curated telemetry to identify abnormal vehicle behavior and groups observations into anomaly episodes.
2. **Real-time fleet alerts:** Reads the Bronze Delta stream, filters records originating from Kafka CDC, evaluates immediate operational thresholds, and writes alert records into `realtime_fleet_alerts`.

These pipelines serve different purposes. Analytical anomaly detection supports historical investigation and episode-level analysis, while real-time alerts support operational monitoring of selected threshold violations.

## Main Objectives

- Parse incoming telemetry records into a structured schema.
- Validate mandatory telemetry fields.
- Separate invalid records from valid records.
- Prevent duplicate records from entering the curated dataset.
- Store cleaned telemetry in Delta tables.
- Organize Silver data using date-based partitioning.
- Enrich telemetry with vehicle reference information.
- Calculate vehicle movement metrics from GPS coordinates.
- Detect excessive idling and cold-chain violations.
- Detect GPS-related and engine-related anomalies.
- Group anomaly observations into episodes.
- Process newly available records incrementally.
- Generate real-time alerts from Kafka CDC telemetry.
- Assign alert categories and severity levels.
- Persist real-time alerts for SQL inspection and downstream monitoring.
- Validate persisted tables using SQL queries.
- Evaluate anomaly detection results against ground-truth data.

---

# 2. Scope of Work

The project includes the following processing responsibilities.

| Area | Description |
|---|---|
| Schema parsing | Convert raw JSON telemetry into structured columns. |
| Data validation | Identify records with missing mandatory values or invalid source status. |
| Rejected records | Preserve rejected telemetry for inspection. |
| Deduplication | Remove repeated records according to configured keys. |
| Incremental processing | Process newly available records and avoid re-ingesting existing identifiers. |
| Delta Lake | Persist curated telemetry and analytical datasets. |
| Partitioning | Organize Silver telemetry and real-time alerts by event date. |
| Enrichment | Add reference attributes through a vehicle-based join. |
| Movement analytics | Calculate distance, time gaps, and estimated speed. |
| Rule-based detection | Identify configured operational anomalies. |
| Episode aggregation | Group related anomalies by vehicle, type, and time gap. |
| Real-time alerting | Detect configured operational threshold violations in Kafka CDC records. |
| Alert classification | Assign alert types according to a priority order. |
| Alert severity | Assign `CRITICAL`, `HIGH`, or `MEDIUM` severity. |
| SQL verification | Inspect the resulting tables. |
| Evaluation | Calculate Precision, Recall, and F1-score using the implemented evaluation logic. |

---

# 3. Technology Stack

| Technology or Concept | Purpose |
|---|---|
| Apache Spark | Distributed data processing. |
| PySpark | Python API for Spark transformations. |
| Spark SQL | Querying processed datasets. |
| Spark Structured Streaming | Incremental processing of incoming data. |
| Delta Lake | Table storage and transactional data operations. |
| Window Functions | Comparing observations belonging to the same vehicle. |
| Broadcast Join | Enriching telemetry with a sufficiently small reference dataset. |
| `foreachBatch` | Applying custom processing logic to each streaming micro-batch. |
| Delta Checkpoints | Tracking streaming progress and supporting recovery. |
| SQL | Validating persisted output tables. |
| Precision, Recall, F1-score | Evaluating anomaly detection performance. |

---

# 4. Processing Architecture

The logical processing architecture includes the Silver transformation pipeline and the real-time alerting pipeline.

```text
                         Bronze Layer
                              |
                              v
                    bronze_telemetry_v2
                              |
              +---------------+----------------+
              |                                |
              v                                v
      Silver Processing                 Real-Time Alerts
              |                                |
              v                                v
       Parse Telemetry                 Parse Telemetry
              |                                |
              v                                v
       Data Validation                 Kafka CDC Filter
              |                                |
       +------+-------+                        v
       |              |                Validate Required Fields
       v              v                        |
 Valid Records   Rejected Records               v
       |              |                Apply Alert Thresholds
       v              v                        |
 Deduplication  silver_rejected                v
       |                                Assign Alert Type
       v                                       |
 silver_telemetry                              v
       |                                Assign Severity
       v                                       |
 Vehicle Enrichment                             v
       |                                realtime_fleet_alerts
       v
 Movement Analytics
       |
       v
 Anomaly Detection
       |
       v
 silver_telemetry_anomalies
       |
       v
 Episode Aggregation
       |
       v
 silver_anomaly_episodes
       |
       v
 Evaluation and SQL
       |
       v
 evaluation_metrics
```

## Main Persisted Datasets

- `bronze_telemetry_v2`
- `silver_telemetry`
- `silver_rejected`
- `silver_telemetry_anomalies`
- `silver_anomaly_episodes`
- `evaluation_metrics`
- `realtime_fleet_alerts`

The availability and exact schema of each table depend on the executed pipeline and the environment in which the tables were created.

The real-time alerting pipeline writes to `realtime_fleet_alerts`. It does not replace the Silver processing or analytical anomaly detection pipeline.

---

# 5. Bronze Layer Inspection

Before applying transformations, the source data should be inspected to understand its structure and identify the fields required by downstream processing.

The Bronze table is the starting point for the Silver transformation and the real-time alerting pipeline.

## 5.1 Read the Bronze Table

```python
df_bronze = spark.table("bronze_telemetry_v2")

df_bronze.show(10, truncate=False)

df_bronze.printSchema()
```

## 5.2 Inspect the Source Data

The inspection step helps verify:

- Whether records are available.
- Which columns exist.
- Whether telemetry is stored as raw JSON or structured columns.
- Which metadata fields are available.
- Whether timestamps and measurement fields require conversion.
- Whether source validation flags and rejection reasons are available.
- Whether the source field identifies Kafka CDC records.

## 5.3 Bronze Sample Data

![Bronze Sample Data](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/01_bronze_sample_data.png?raw=true)

The screenshot documents the source records before the complete Silver transformation.

---

# 6. Bronze-to-Silver Processing

The Bronze-to-Silver stage converts ingested telemetry into a structured dataset that can be validated, deduplicated, and stored in the Silver layer.

The transformation includes:

1. Reading the Bronze table.
2. Parsing raw telemetry.
3. Converting fields to the expected data types.
4. Checking mandatory fields.
5. Separating valid and rejected records.
6. Deduplicating incoming data.
7. Identifying records that have not already been processed.
8. Writing valid records to the Silver table.

The real-time alerting pipeline independently reads the same Bronze source and applies its own source filter and alert-specific rules.

## 6.1 Processing Requirements

The pipeline needs to distinguish between two types of problems.

**Structural problems** occur when a record cannot be parsed correctly or expected fields are missing.

**Data quality problems** occur when a record is parsed but does not satisfy the required validation conditions.

Separating these cases makes the processing output easier to inspect and troubleshoot.

## 6.2 Bronze-to-Silver Processing Evidence

![Bronze-to-Silver Incremental Data Processing and Schema Parsing](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/02_Bronze-to-Silver%20Incremental%20data%20Processing%20and%20Schema%20Parsing.png?raw=true)

---

# 7. Schema Parsing

The processing implementations define an explicit schema for the telemetry JSON payload.

An explicit schema helps Spark interpret source fields without relying on inference for every micro-batch.

## 7.1 Define the Telemetry Schema

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

telemetry_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("engine_temp_c", DoubleType(), True),
    StructField("fuel_rate_lph", DoubleType(), True),
    StructField("odometer_km", DoubleType(), True),
    StructField("cargo_temp_c", DoubleType(), True)
])
```

## 7.2 Parse the JSON Payload

The `from_json` function converts the raw JSON string into a structured column.

```python
df_parsed = microBatchDF.withColumn(
    "parsed_data",
    F.from_json(F.col("raw"), telemetry_schema)
)
```

After parsing, the nested fields can be selected as regular columns.

```python
df_parsed = df_parsed.select(
    F.col("db_id"),
    F.col("source"),
    F.col("is_valid"),
    F.col("reject_reason"),
    F.col("EventEnqueuedUtcTime"),
    F.col("parsed_data.*")
)
```

The real-time alerting pipeline selects only the metadata and telemetry fields required for alert generation.

## 7.3 Convert the Event Timestamp

The schema defines `event_time` as a string because the source JSON timestamp is initially represented as text.

The pipeline converts it to a timestamp:

```python
df_parsed = df_parsed.withColumn(
    "event_time",
    F.col("event_time").cast("timestamp")
)
```

The converted timestamp is required for chronological ordering, movement calculations, date partitioning, alert timestamps, and anomaly episode grouping.

## 7.4 Schema Parsing Evidence

![Bronze-to-Silver Incremental Data Processing and Schema Parsing](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/02_Bronze-to-Silver%20Incremental%20data%20Processing%20and%20Schema%20Parsing.png?raw=true)

---

# 8. Data Validation

Data validation identifies records that do not contain the mandatory values required for downstream processing.

For the Silver processing workflow, the mandatory fields include:

- `event_id`
- `vehicle_id`
- `event_time`
- `latitude`
- `longitude`
- `speed_kmh`

The real-time alerting pipeline uses a narrower validation condition because it evaluates operational thresholds rather than performing all Silver-layer calculations.

## 8.1 Silver Validation Condition

```python
validation_condition = (
    F.col("event_id").isNotNull()
    & F.col("vehicle_id").isNotNull()
    & F.col("event_time").isNotNull()
    & F.col("latitude").isNotNull()
    & F.col("longitude").isNotNull()
    & F.col("speed_kmh").isNotNull()
)
```

The condition evaluates to true only when all listed mandatory fields are present.

## 8.2 Real-Time Alert Validation

The real-time alerting pipeline requires:

```python
df_valid = df_parsed.filter(
    (F.col("source") == "kafka_cdc")
    & F.col("event_id").isNotNull()
    & F.col("vehicle_id").isNotNull()
    & F.col("event_time").isNotNull()
)
```

This filter restricts the real-time alerting pipeline to Kafka CDC records with an event identifier, vehicle identifier, and valid parsed event timestamp.

Latitude, longitude, and speed are not mandatory in this specific filter. However, the alert rules depend on the availability of their respective measurement fields.

For example, a null `speed_kmh` cannot satisfy a condition such as `speed_kmh > 105`.

## 8.3 Why Validate Before Processing?

Missing mandatory values can affect later calculations.

For example:

- A missing vehicle identifier prevents reliable vehicle-level grouping.
- A missing event timestamp prevents reliable chronological ordering.
- Missing GPS coordinates prevent distance calculation.
- Missing speed prevents the configured reported-speed checks.
- Missing cargo temperature prevents evaluation of the cold-chain condition.

## 8.4 Validation Evidence

![Data Validation, Deduplication, and Rejected Records Handling](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/03_Data%20Validation%2C%20Deduplication%2C%20and%20Rejected%20Records%20Handling.png?raw=true)

---

# 9. Rejected Records Handling

Invalid telemetry records should be preserved separately when the pipeline needs to support data quality investigation.

The Silver processing workflow considers both the source validity flag and the required-field validation condition.

## 9.1 Identify Rejected Records

```python
df_rejected = df_deduplicated_mb.filter(
    (F.col("is_valid") == False)
    | (~validation_condition)
)
```

A record is rejected if its source validity flag is false or if one or more mandatory fields are missing.

## 9.2 Assign a Rejection Reason

The implementation retains an existing rejection reason where available. Otherwise, it assigns a fallback description.

```python
df_rejected = df_rejected.withColumn(
    "rejection_reason",
    F.coalesce(
        F.col("reject_reason"),
        F.lit(
            "missing mandatory fields (null keys/coords/speed)"
        )
    )
).withColumn(
    "rejected_at",
    F.current_timestamp()
)
```

## 9.3 Write Rejected Records

```python
if not df_rejected.isEmpty():
    df_rejected.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("silver_rejected")
```

The rejected records are appended to `silver_rejected` when the micro-batch contains rejected data.

The real-time alerting pipeline does not write its filtered-out records to `silver_rejected`. It simply excludes records that do not satisfy its source and required-field filters.

## 9.4 Benefits of a Rejected Records Table

- Invalid data remains available for investigation.
- Rejection reasons can be inspected.
- Data quality problems can be quantified.
- Invalid records can be separated from analytical data.
- Source issues can be investigated without contaminating the valid Silver dataset.

## 9.5 Rejected Records Evidence

![Data Validation, Deduplication, and Rejected Records Handling](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/03_Data%20Validation%2C%20Deduplication%2C%20and%20Rejected%20Records%20Handling.png?raw=true)

---

# 10. Deduplication

Deduplication prevents repeated observations from unnecessarily increasing record counts or distorting downstream calculations.

## 10.1 Micro-Batch Deduplication

The Silver processing pipeline removes duplicate source records using `db_id`.

```python
df_deduplicated_mb = df_parsed.dropDuplicates(
    ["db_id"]
)
```

This removes repeated `db_id` values within the current micro-batch.

## 10.2 Business-Key Deduplication

The valid dataset also removes duplicate observations based on the combination of vehicle identifier and event timestamp.

```python
df_valid_clean = df_valid.dropDuplicates(
    ["vehicle_id", "event_time"]
).withColumn(
    "event_date",
    F.to_date(F.col("event_time"))
)
```

This rule treats records with the same `vehicle_id` and `event_time` as duplicates within the dataframe being processed.

## 10.3 Why Use Different Keys?

| Key | Purpose |
|---|---|
| `db_id` | Identifies a source record for ingestion-level deduplication. |
| `vehicle_id`, `event_time` | Identifies a telemetry observation according to the configured business rule. |

The correct key depends on the source system and the intended definition of a unique telemetry event.

The current real-time alerting pipeline does not apply explicit deduplication to the alert output. If the same logical event is processed again, it may be appended again unless the source/checkpoint behavior or another idempotency mechanism prevents it.

## 10.4 Deduplication Evidence

![Silver Layer Deduplication and Date-Based Partitioning](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/04_Silver%20Layer%20Deduplication%20and%20Date-Based%20Partitioning.png?raw=true)

---

# 11. Incremental Processing and Duplicate Prevention

Incremental ingestion processes newly available records without blindly appending every record from the source.

The Silver processing implementation checks whether incoming valid records already exist in the Silver dataset.

## 11.1 Check Whether the Silver Table Exists

```python
if spark.catalog.tableExists("silver_telemetry"):
    silver_existing = spark.read.table(
        "silver_telemetry"
    ).select("db_id")
```

If the table exists, its existing `db_id` values are used as a reference for duplicate prevention.

## 11.2 Use a Left Anti Join

```python
new_valid_records = df_valid_clean.join(
    silver_existing,
    on="db_id",
    how="left_anti"
)
```

A left anti join returns rows from the incoming dataset that do not have a matching `db_id` in the existing Silver dataset.

## 11.3 Handle the First Write

If the Silver table does not yet exist, the incoming cleaned records can be used as the initial dataset.

```python
else:
    new_valid_records = df_valid_clean
```

## 11.4 Append New Records

```python
if not new_valid_records.isEmpty():
    new_valid_records.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("event_date") \
        .saveAsTable("silver_telemetry")
```

## 11.5 Important Implementation Detail

The existing-table anti join prevents repeated `db_id` values from being inserted when the check is applied successfully.

However:

- The check is based on `db_id`.
- It does not independently guarantee that `(vehicle_id, event_time)` is unique across all previously written data.
- `dropDuplicates` inside a micro-batch does not automatically deduplicate records across different micro-batches.
- Separate writes to multiple Delta tables inside one `foreachBatch` callback are not one cross-table atomic transaction.

These distinctions matter when assessing end-to-end duplicate prevention.

---

# 12. Silver Table Partitioning

The Silver telemetry table is partitioned by `event_date`.

## 12.1 Create the Event Date Column

```python
df_valid_clean = df_valid_clean.withColumn(
    "event_date",
    F.to_date(F.col("event_time"))
)
```

## 12.2 Write the Partitioned Table

```python
new_valid_records.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("event_date") \
    .saveAsTable("silver_telemetry")
```

## 12.3 Why Partition by Event Date?

Date-based partitioning can improve data organization for workloads that frequently filter on the event date.

Potential benefits include:

- Organizing records into date-based partitions.
- Supporting partition pruning when queries filter by `event_date`.
- Making date-oriented analytical workloads easier to manage.
- Supporting incremental writes into the curated table.

The actual performance benefit depends on data volume, partition distribution, and query patterns.

## 12.4 Partitioning Evidence

![Silver Layer Deduplication and Date-Based Partitioning](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/04_Silver%20Layer%20Deduplication%20and%20Date-Based%20Partitioning.png?raw=true)

---

# 13. Telemetry Enrichment

Telemetry enrichment adds contextual information to processed vehicle observations.

A reference dataset can provide additional attributes associated with each vehicle.

When the reference dataset is small enough to fit efficiently in executor memory, a broadcast join can reduce shuffle requirements.

## 13.1 Broadcast Join

```python
df_enriched = df_silver.join(
    F.broadcast(df_vehicle_reference),
    on="vehicle_id",
    how="left"
)
```

This example assumes that `df_silver` and `df_vehicle_reference` have already been created and that `vehicle_id` is a compatible join key in both datasets.

## 13.2 Why Use a Left Join?

A left join retains telemetry records even if a matching reference record is not found.

This can be useful when missing reference data should not automatically discard otherwise valid telemetry.

## 13.3 Why Use a Broadcast Join?

Broadcast joins are useful when one side of the join is sufficiently small.

Spark can distribute the smaller reference dataset to the executors, potentially avoiding a large shuffle of the telemetry data.

Broadcasting an oversized dataset can increase memory pressure, so the suitability of the join depends on the actual reference table size.

## 13.4 Enrichment Evidence

![Silver Telemetry Enrichment Using Broadcast Join](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/05_Silver%20Telemetry%20Enrichment%20Using%20Broadcast%20Join.png?raw=true)

---

# 14. Vehicle Movement Analytics

Movement analytics derives additional information from consecutive observations of the same vehicle.

The calculations use GPS coordinates and timestamps to estimate distance traveled and movement speed.

## 14.1 Create a Window Specification

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

window_spec = Window \
    .partitionBy("vehicle_id") \
    .orderBy("event_time")
```

The window partitions observations by vehicle and orders them chronologically.

## 14.2 Retrieve Previous Coordinates and Timestamp

```python
df_windowed = df_enriched \
    .withColumn(
        "prev_latitude",
        F.lag("latitude").over(window_spec)
    ) \
    .withColumn(
        "prev_longitude",
        F.lag("longitude").over(window_spec)
    ) \
    .withColumn(
        "prev_event_time",
        F.lag("event_time").over(window_spec)
    )
```

The `lag` function retrieves values from the previous row in the window.

The first observation for each vehicle has no preceding observation, so its previous values are null.

## 14.3 Calculate the Time Gap

```python
df_windowed = df_windowed.withColumn(
    "time_gap_sec",
    F.col("event_time").cast("long")
    - F.col("prev_event_time").cast("long")
)
```

The time gap is measured in seconds.

## 14.4 Why Use Window Functions?

Window functions allow calculations across related rows without collapsing the dataset into one aggregate row per vehicle.

They are useful for:

- Retrieving previous observations.
- Calculating time differences.
- Comparing current and previous GPS coordinates.
- Deriving movement metrics.
- Identifying changes between consecutive telemetry events.

## 14.5 Movement Analytics Evidence

![Vehicle Movement Analytics and Cold Chain Violation Detection Using PySpark](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/06_Vehicle%20Movement%20Analytics%20and%20Cold%20Chain%20Violation%20Detection%20Using%20PySpark.png?raw=true)

---

# 15. Distance and Speed Calculation

The movement-processing stage estimates the distance between consecutive GPS observations and derives a calculated speed.

## 15.1 Haversine Formula

The Haversine formula estimates the great-circle distance between two geographic coordinates.

```text
a = sin²((lat2 - lat1) / 2)
    + cos(lat1) * cos(lat2)
    * sin²((lon2 - lon1) / 2)

distance_km = 2 * R * asin(sqrt(a))
```

Where:

- `lat1` is the previous latitude.
- `lon1` is the previous longitude.
- `lat2` is the current latitude.
- `lon2` is the current longitude.
- `R` is the approximate Earth radius in kilometers, 6371 km.
- Latitude and longitude must be expressed in radians.

## 15.2 Distance Calculation Considerations

Distance calculations depend on the availability of both current and previous coordinates.

The calculation also needs to account for:

- Missing coordinates.
- Duplicate timestamps.
- Non-positive time gaps.
- Irregular sampling intervals.
- GPS measurement errors.
- Unrealistic jumps between positions.

## 15.3 Calculated Speed

Conceptually, the speed calculation is:

```text
calculated_speed_kmh = distance_km / time_gap_hours
```

Equivalently:

```text
calculated_speed_kmh = distance_km * 3600 / time_gap_sec
```

In the implemented movement logic, observations are eligible for calculation when:

- The time gap is greater than 3 seconds.
- The time gap is no greater than 3600 seconds.
- The distance is at least 0.01 km.

## 15.4 Speed Filtering

The processing logic sets calculated speeds greater than 180 km/h to zero.

This is an implementation-specific filtering rule and should be considered when interpreting downstream anomaly detection.

Values above the filtering threshold may no longer retain their original calculated speed after this transformation.

## 15.5 Reported Speed Versus Calculated Speed

Reported speed comes from the telemetry field `speed_kmh`.

Calculated speed is derived from GPS distance and the time between observations.

These are different measurements and should not be treated as interchangeable without considering their calculation and validation rules.

---

# 16. Idling and Cold-Chain Detection

The movement analytics stage derives operational indicators useful for vehicle monitoring.

## 16.1 Excessive Idling

The configured idling condition is:

```python
is_idling = (
    (F.col("speed_kmh") < 2)
    & (F.col("rpm") > 0)
)
```

The condition identifies observations where the reported vehicle speed is below 2 km/h while RPM is greater than zero.

This is a rule-based indicator. It does not, by itself, establish the duration of an idling event.

## 16.2 Analytical Cold-Chain Violation

The analytical cold-chain condition is:

```python
is_cold_chain_violation = (
    (F.col("cargo_temp_c") < -2)
    | (F.col("cargo_temp_c") > 12)
)
```

The condition identifies cargo-temperature readings below -2°C or above 12°C.

The configured range is specific to the analytical project and should not be treated as a universal temperature requirement for every cargo type.

## 16.3 Real-Time Cold-Chain Threshold

The new real-time alerting pipeline uses a different configured range:

```python
cold_chain_condition = (
    F.col("cargo_temp_c").isNotNull()
    & (
        (F.col("cargo_temp_c") < 2)
        | (F.col("cargo_temp_c") > 8)
    )
)
```

This real-time condition identifies cargo temperatures below 2°C or above 8°C.

**Important distinction:** The analytical cold-chain rule and the real-time alerting rule use different thresholds.

| Pipeline | Lower threshold | Upper threshold |
|---|---:|---:|
| Analytical anomaly detection | -2°C | 12°C |
| Real-time fleet alerts | 2°C | 8°C |

The difference is intentional in the current implementation. The thresholds should be aligned if both pipelines are expected to follow the same operational requirements.

## 16.4 Operational Use

These indicators can help identify:

- Potential excessive idling.
- Cargo-temperature excursions.
- Vehicle operating conditions requiring further inspection.
- Observations that may need to be grouped into longer anomaly episodes.

## 16.5 Cold-Chain and Movement Evidence

![Vehicle Movement Analytics and Cold Chain Violation Detection Using PySpark](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/06_Vehicle%20Movement%20Analytics%20and%20Cold%20Chain%20Violation%20Detection%20Using%20PySpark.png?raw=true)

---

# 17. Anomaly Detection Rules

The analytical anomaly detection stage evaluates telemetry observations against the project's configured operational rules.

The output records retain relevant telemetry and calculated fields alongside anomaly indicators.

## 17.1 Engine Overheating

```python
engine_overheating_condition = (
    F.col("engine_temp_c") > 105
)
```

The condition is true when engine temperature exceeds 105°C.

## 17.2 GPS Teleportation

```python
gps_teleportation_condition = (
    (F.col("calculated_speed_kmh") > 100)
    & (F.col("distance_km") > 0.5)
)
```

The condition identifies an observation with calculated speed above 100 km/h and distance greater than 0.5 km.

The rule is intended to identify suspicious movement observations. It is not, by itself, proof of GPS spoofing or a real-world vehicle teleportation event.

## 17.3 RPM Spike

```python
rpm_spike_condition = (
    F.col("rpm") > 4500
)
```

The condition is true when RPM exceeds 4500.

## 17.4 Cold-Chain Breach

```python
cold_chain_breach_condition = (
    F.col("is_cold_chain_violation")
)
```

## 17.5 Excessive Idling

```python
excessive_idling_condition = (
    F.col("is_idling")
)
```

## 17.6 Combined Anomaly Indicator

Conceptually, the overall anomaly indicator is:

```python
is_anomaly = (
    cold_chain_breach_condition
    | excessive_idling_condition
    | gps_teleportation_condition
    | engine_overheating_condition
    | rpm_spike_condition
)
```

The exact implementation should use the individual flag columns created by the movement and anomaly processing stages.

## 17.7 Anomaly Rules Summary

| Anomaly | Configured condition |
|---|---|
| Engine overheating | `engine_temp_c > 105` |
| GPS teleportation | `calculated_speed_kmh > 100` and `distance_km > 0.5` |
| RPM spike | `rpm > 4500` |
| Cold-chain breach | `is_cold_chain_violation` is true |
| Excessive idling | `is_idling` is true |

These are the configured project rules, not universal operating limits for all vehicles.

## 17.8 Anomaly Detection Evidence

![Anomaly Detection and Episode Aggregation for Vehicle Telemetry](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/07_Anomaly%20Detection%20and%20Episode%20Aggregation%20for%20Vehicle%20Telemetry.png?raw=true)

---

# 18. Anomaly Type Assignment

An observation can satisfy more than one anomaly rule.

When a single anomaly category is assigned, the configured priority order determines which category is selected.

The analytical implementation uses the following priority:

1. `cold_chain_breach`
2. `excessive_idling`
3. `route_deviation`
4. `engine_overheating`
5. `rpm_spike`

## 18.1 Priority-Based Classification

```python
anomaly_type = (
    F.when(
        F.col("is_cold_chain_violation"),
        "cold_chain_breach"
    )
    .when(
        F.col("is_idling"),
        "excessive_idling"
    )
    .when(
        F.col("is_gps_teleportation"),
        "route_deviation"
    )
    .when(
        F.col("is_engine_overheating"),
        "engine_overheating"
    )
    .when(
        F.col("is_rpm_spike"),
        "rpm_spike"
    )
)
```

The flag names in this illustrative example should match the actual columns created in the implementation.

A priority-based classifier assigns one category to an observation when multiple conditions are true. If the project needs to retain every simultaneously occurring anomaly, the output design should preserve the individual flags or produce multiple anomaly records per observation.

## 18.2 Analytical Anomaly Classification Versus Real-Time Alert Classification

The real-time alerting pipeline uses a separate classification priority:

1. `COLD_CHAIN_VIOLATION`
2. `ENGINE_OVERHEATING`
3. `CRITICAL_SPEEDING`

The two classification systems have different purposes and categories.

The analytical pipeline can identify several types of anomalies, including idling, GPS-related anomalies, and RPM spikes. The real-time alerting pipeline focuses on three operational conditions.

---

# 19. Anomaly Episode Aggregation

Anomaly detection identifies individual anomalous observations. Episode aggregation groups related observations into longer incidents.

This creates a more useful operational view than treating every anomalous reading as an independent incident.

## 19.1 Episode Grouping Keys

The episode workflow groups records using:

- `vehicle_id`
- `anomaly_type`
- `event_time`

Records are ordered by event time within each vehicle and anomaly type.

## 19.2 Window Specification

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

episode_window = Window \
    .partitionBy(
        "vehicle_id",
        "anomaly_type"
    ) \
    .orderBy("event_time")
```

## 19.3 Retrieve the Previous Event Time

```python
df_episodes = df_anomalies.withColumn(
    "prev_event_time",
    F.lag("event_time").over(episode_window)
)
```

## 19.4 Calculate the Time Gap

```python
df_episodes = df_episodes.withColumn(
    "gap_minutes",
    (
        F.col("event_time").cast("long")
        - F.col("prev_event_time").cast("long")
    ) / 60
)
```

## 19.5 Identify Episode Boundaries

The configured episode rule starts a new episode when the previous event does not exist or the time gap is greater than 15 minutes.

```python
new_episode_condition = (
    F.col("prev_event_time").isNull()
    | (F.col("gap_minutes") > 15)
)
```

A cumulative sum of the new-episode indicator can then assign an episode identifier within each partition.

## 19.6 Aggregate Episode Metrics

Typical metrics include:

- Episode start time.
- Episode end time.
- Number of readings.
- Maximum calculated speed.
- Duration in seconds.

```python
df_episode_summary = df_episodes.groupBy(
    "vehicle_id",
    "anomaly_type",
    "episode_id"
).agg(
    F.min("event_time").alias("start_time"),
    F.max("event_time").alias("end_time"),
    F.count("*").alias("reading_count"),
    F.max("calculated_speed_kmh").alias(
        "max_calculated_speed"
    )
)
```

The episode identifier must be generated before grouping, and its grouping keys must uniquely identify an episode within the intended scope.

## 19.7 Episode Duration

```text
duration_seconds = end_time - start_time
```

A single-reading episode may have a duration of zero when the start and end timestamps are equal.

## 19.8 Episode Aggregation Evidence

![Anomaly Detection and Episode Aggregation for Vehicle Telemetry](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/07_Anomaly%20Detection%20and%20Episode%20Aggregation%20for%20Vehicle%20Telemetry.png?raw=true)

---

# 20. Incremental Silver Streaming Pipeline

The Silver processing implementation uses Spark Structured Streaming to process records from the Bronze Delta table.

The pipeline processes each micro-batch using a `foreachBatch` function.

## 20.1 Read the Bronze Stream

```python
df_bronze_stream = spark.readStream \
    .format("delta") \
    .option("ignoreChanges", "true") \
    .table("bronze_telemetry_v2")
```

This creates a streaming DataFrame from the Bronze Delta table.

The `ignoreChanges` option affects how updates to the source Delta table are handled. It should not be interpreted as a guarantee that every changed source row will be reprocessed as a new event.

## 20.2 Process Each Micro-Batch

The micro-batch function implements:

1. Raw JSON parsing.
2. Timestamp conversion.
3. Mandatory-field validation.
4. Source-level validation.
5. Rejected-record handling.
6. Deduplication.
7. Existing-record checks.
8. Incremental writes to the Silver tables.

## 20.3 Start the Streaming Query

```python
query = df_bronze_stream.writeStream \
    .foreachBatch(
        process_silver_streaming_microbatch
    ) \
    .option(
        "checkpointLocation",
        "Files/checkpoints/silver_streaming_full_rules"
    ) \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
```

The function `process_silver_streaming_microbatch` must be defined before this query is started.

## 20.4 Processing Sequence

For each micro-batch, the function:

1. Checks whether the batch is empty.
2. Parses the raw JSON payload.
3. Selects source metadata and telemetry fields.
4. Converts `event_time` to a timestamp.
5. Defines the mandatory-field validation condition.
6. Deduplicates records using `db_id`.
7. Separates rejected records.
8. Appends rejected records to `silver_rejected`.
9. Filters valid records.
10. Removes duplicates using `vehicle_id` and `event_time` within the cleaned batch.
11. Creates the `event_date` partitioning column.
12. Checks whether `silver_telemetry` exists.
13. Uses a left anti join to exclude existing `db_id` values.
14. Appends new valid records to the partitioned Silver table.

## 20.5 Streaming Considerations

- The checkpoint location must be accessible to the Spark execution environment.
- The source table must support the configured streaming read.
- The target tables must exist or be creatable in the execution environment.
- The schema used by `from_json` must match the incoming payload.
- Duplicate prevention should be tested across repeated runs and across micro-batches.
- Append operations should be designed with retry behavior in mind.
- `availableNow=True` processes currently available input and terminates rather than remaining active indefinitely.
- Reusing an existing checkpoint preserves the stream's recorded progress. Clearing a checkpoint or changing checkpoint paths can cause previously processed input to be processed again, depending on the source and query configuration.

## 20.6 Streaming Pipeline Evidence

![Incremental Streaming Ingestion with Duplicate Prevention in the Silver Layer](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/10_Incremental%20Streaming%20Ingestion%20with%20Duplicate%20Prevention%20in%20the%20Silver%20Layer.png?raw=true)

---

# 21. Real-Time Fleet Alerts Pipeline

The real-time alerting pipeline consumes the Bronze Delta stream and generates operational alerts from records originating from Kafka CDC.

The pipeline applies a focused set of rules:

1. Cold-chain violation.
2. Engine overheating.
3. Critical speeding.

The resulting alerts are written to `realtime_fleet_alerts`, which can be queried by downstream dashboards or operational monitoring tools.

## 21.1 Real-Time Alerting Workflow

```text
                bronze_telemetry_v2
                         |
                         v
                 Delta Streaming Read
                         |
                         v
                    Parse JSON
                         |
                         v
                 Convert event_time
                         |
                         v
                 Filter kafka_cdc
                         |
                         v
              Validate Required Fields
                         |
                         v
                Apply Alert Conditions
                         |
                         v
                Assign alert_type
                         |
                         v
                 Assign severity
                         |
                         v
               Add Processing Timestamp
                         |
                         v
                   Add event_date
                         |
                         v
               realtime_fleet_alerts
```

## 21.2 Telemetry Schema

The real-time pipeline uses the following telemetry schema:

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

telemetry_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("engine_temp_c", DoubleType(), True),
    StructField("fuel_rate_lph", DoubleType(), True),
    StructField("odometer_km", DoubleType(), True),
    StructField("cargo_temp_c", DoubleType(), True)
])
```

## 21.3 Read the Bronze Delta Stream

```python
df_bronze_stream = spark.readStream \
    .format("delta") \
    .option("ignoreChanges", "true") \
    .table("bronze_telemetry_v2")
```

The source is the existing Bronze Delta table.

The stream uses the `raw` field to parse telemetry JSON and uses the `source` field to select Kafka CDC records.

## 21.4 Target Table Preparation

The original implementation truncates the target table if it exists:

```python
if spark.catalog.tableExists("realtime_fleet_alerts"):
    spark.sql("TRUNCATE TABLE realtime_fleet_alerts")
    print("Table 'realtime_fleet_alerts' truncated successfully!")
```

This operation removes the existing alert records before the subsequent stream starts.

**Important operational note:** Truncating the table is appropriate only when a full refresh is intended. It is not suitable for an ordinary incremental production run if historical alerts must be preserved.

The streaming checkpoint may still record that the source data has already been processed. Consequently, truncating the target table while reusing an existing checkpoint can leave the target empty or incomplete because previously processed source data may not be replayed.

For an incremental production pipeline, the target should normally not be truncated at startup. For a deliberate full refresh, the target reset and checkpoint reset/replay strategy must be planned together.

## 21.5 Define the Micro-Batch Function

The function processes each available micro-batch.

```python
from pyspark.sql import functions as F


def process_realtime_alerts_microbatch(microBatchDF, batchId):

    if microBatchDF.isEmpty():
        return

    # Parse raw telemetry JSON.
    df_parsed = microBatchDF.withColumn(
        "parsed_data",
        F.from_json(
            F.col("raw"),
            telemetry_schema
        )
    ).select(
        F.col("db_id"),
        F.col("source"),
        F.col("EventEnqueuedUtcTime"),
        F.col("parsed_data.*")
    ).withColumn(
        "event_time",
        F.col("event_time").cast("timestamp")
    )

    # Only Kafka CDC records with the required identifiers
    # and a successfully parsed event timestamp are processed.
    df_valid = df_parsed.filter(
        (F.col("source") == "kafka_cdc")
        & F.col("event_id").isNotNull()
        & F.col("vehicle_id").isNotNull()
        & F.col("event_time").isNotNull()
    )

    if df_valid.isEmpty():
        return

    # ---------------------------------------------------------
    # Real-Time Incident Rules — Kafka CDC Stream Only
    #
    # Priority 1: COLD_CHAIN_VIOLATION
    # Priority 2: ENGINE_OVERHEATING
    # Priority 3: CRITICAL_SPEEDING
    # ---------------------------------------------------------

    cold_chain_condition = (
        F.col("cargo_temp_c").isNotNull()
        & (
            (F.col("cargo_temp_c") < 2)
            | (F.col("cargo_temp_c") > 8)
        )
    )

    engine_overheating_condition = (
        F.col("engine_temp_c") > 105
    )

    critical_speeding_condition = (
        F.col("speed_kmh") > 105
    )

    df_alerts = df_valid.filter(
        critical_speeding_condition
        | cold_chain_condition
        | engine_overheating_condition
    ).withColumn(
        "alert_type",
        F.when(
            cold_chain_condition,
            F.lit("COLD_CHAIN_VIOLATION")
        ).when(
            engine_overheating_condition,
            F.lit("ENGINE_OVERHEATING")
        ).otherwise(
            F.lit("CRITICAL_SPEEDING")
        )
    ).withColumn(
        "severity",
        F.when(
            (F.col("speed_kmh") > 110)
            | (F.col("engine_temp_c") > 115),
            F.lit("CRITICAL")
        ).when(
            F.col("speed_kmh") > 105,
            F.lit("HIGH")
        ).otherwise(
            F.lit("MEDIUM")
        )
    ).withColumn(
        "alert_processed_at",
        F.current_timestamp()
    ).withColumn(
        "event_date",
        F.to_date(F.col("event_time"))
    ).select(
        "event_id",
        "vehicle_id",
        "trip_id",
        "event_time",
        "speed_kmh",
        "engine_temp_c",
        "cargo_temp_c",
        "latitude",
        "longitude",
        "alert_type",
        "severity",
        "alert_processed_at",
        "event_date"
    )

    if not df_alerts.isEmpty():
        df_alerts.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("event_date") \
            .saveAsTable("realtime_fleet_alerts")
```

## 21.6 Alert Rule 1 — Cold-Chain Violation

```python
cold_chain_condition = (
    F.col("cargo_temp_c").isNotNull()
    & (
        (F.col("cargo_temp_c") < 2)
        | (F.col("cargo_temp_c") > 8)
    )
)
```

The condition is true when cargo temperature is outside the configured 2°C–8°C range.

The explicit null check ensures that a missing cargo-temperature value is not treated as a cold-chain violation by this rule.

The threshold range is specific to the current implementation and should be aligned with the applicable cargo requirements before operational use.

## 21.7 Alert Rule 2 — Engine Overheating

```python
engine_overheating_condition = (
    F.col("engine_temp_c") > 105
)
```

An engine temperature greater than 105°C satisfies the overheating rule.

The alert condition and the severity condition are separate. An overheating alert may be assigned a severity according to the severity logic even when the engine temperature is not greater than 115°C.

## 21.8 Alert Rule 3 — Critical Speeding

```python
critical_speeding_condition = (
    F.col("speed_kmh") > 105
)
```

The rule identifies records with reported vehicle speed greater than 105 km/h.

The alert name is `CRITICAL_SPEEDING`, although the severity field may be `HIGH` or `CRITICAL` depending on the separate severity thresholds.

## 21.9 Alert Type Priority

An observation may satisfy more than one alert condition.

The implementation assigns one alert type according to this order:

1. `COLD_CHAIN_VIOLATION`
2. `ENGINE_OVERHEATING`
3. `CRITICAL_SPEEDING`

The classification uses chained `when` expressions:

```python
df_alerts = df_alerts.withColumn(
    "alert_type",
    F.when(
        cold_chain_condition,
        F.lit("COLD_CHAIN_VIOLATION")
    ).when(
        engine_overheating_condition,
        F.lit("ENGINE_OVERHEATING")
    ).otherwise(
        F.lit("CRITICAL_SPEEDING")
    )
)
```

For example, if one event has an out-of-range cargo temperature and an engine temperature above 105°C, the selected alert type is `COLD_CHAIN_VIOLATION` because that condition has the highest priority.

This priority determines the category stored in `alert_type`. It does not remove the other measurements from the alert record.

## 21.10 Alert Severity Assignment

The severity logic uses the following conditions:

```python
severity = (
    F.when(
        (F.col("speed_kmh") > 110)
        | (F.col("engine_temp_c") > 115),
        F.lit("CRITICAL")
    )
    .when(
        F.col("speed_kmh") > 105,
        F.lit("HIGH")
    )
    .otherwise(
        F.lit("MEDIUM")
    )
)
```

The severity is assigned according to the first matching condition.

| Priority | Condition | Severity |
|---|---|---|
| 1 | Speed greater than 110 km/h OR engine temperature greater than 115°C | `CRITICAL` |
| 2 | Speed greater than 105 km/h | `HIGH` |
| 3 | Any remaining generated alert | `MEDIUM` |

The `otherwise("MEDIUM")` branch means that a cold-chain violation or engine-overheating alert can receive `MEDIUM` severity when neither of the first two severity conditions is satisfied.

This is the behavior of the configured code. If severity is intended to depend directly on the selected `alert_type`, the severity logic must be redesigned accordingly.

## 21.11 Add Alert Processing Metadata

The pipeline adds a processing timestamp:

```python
.withColumn(
    "alert_processed_at",
    F.current_timestamp()
)
```

This timestamp indicates when the alert transformation was evaluated by Spark. It is not the original event time.

The pipeline also derives the event date:

```python
.withColumn(
    "event_date",
    F.to_date(F.col("event_time"))
)
```

The `event_date` field is used for partitioning the target table.

## 21.12 Real-Time Alert Output Schema

The output includes the following columns:

| Column | Description |
|---|---|
| `event_id` | Identifier of the telemetry event. |
| `vehicle_id` | Identifier of the vehicle. |
| `trip_id` | Identifier of the associated trip, if available. |
| `event_time` | Timestamp of the original telemetry event. |
| `speed_kmh` | Reported vehicle speed in kilometers per hour. |
| `engine_temp_c` | Reported engine temperature in Celsius. |
| `cargo_temp_c` | Reported cargo temperature in Celsius. |
| `latitude` | Latitude from the telemetry event. |
| `longitude` | Longitude from the telemetry event. |
| `alert_type` | Selected alert category. |
| `severity` | Assigned alert severity. |
| `alert_processed_at` | Timestamp when the alert transformation was processed. |
| `event_date` | Date derived from `event_time`, used for partitioning. |

The `db_id`, `source`, and `EventEnqueuedUtcTime` fields are used or available during parsing but are not included in the final selected alert output.

## 21.13 Write Alerts to Delta Lake

```python
if not df_alerts.isEmpty():
    df_alerts.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("event_date") \
        .saveAsTable("realtime_fleet_alerts")
```

The write operation:

1. Skips the write if the batch contains no alerts.
2. Uses Delta Lake as the target format.
3. Appends records to the target table.
4. Allows compatible schema changes through `mergeSchema`.
5. Partitions the target table by `event_date`.
6. Saves the data in `realtime_fleet_alerts`.

The append operation does not independently enforce uniqueness of `event_id`. A production implementation should define the expected replay and deduplication behavior.

## 21.14 Start the Real-Time Streaming Query

```python
query = df_bronze_stream.writeStream \
    .foreachBatch(
        process_realtime_alerts_microbatch
    ) \
    .option(
        "checkpointLocation",
        "Files/checkpoints/realtime_alerts_v8"
    ) \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
```

The function `process_realtime_alerts_microbatch` must be defined before starting the query.

The checkpoint path tracks the streaming query's progress. It should remain stable between normal runs of the same query.

The `availableNow=True` trigger processes currently available input and then terminates. It does not keep the query running indefinitely to process future records.

For continuous real-time monitoring, use a trigger and deployment pattern appropriate for a long-running streaming query rather than an available-now batch.

## 21.15 Real-Time Alerting Pipeline: Operational Notes

### Source filtering

Only records where `source == "kafka_cdc"` are considered for alert generation.

### Required fields

Records must contain `event_id`, `vehicle_id`, and a successfully parsed `event_time`.

### Null measurement fields

The alert filter relies on Spark comparison behavior. A null speed or engine temperature does not satisfy its greater-than condition. The cold-chain rule explicitly checks that `cargo_temp_c` is not null.

### Alert priority

Only one `alert_type` is assigned to each generated alert row. The first matching `when` condition wins.

### Severity

Severity is assigned independently from alert type. A cold-chain violation may be `MEDIUM`, `HIGH`, or `CRITICAL` depending on the speed and engine-temperature values.

### Append behavior

The pipeline appends generated alerts. It does not use a Delta `MERGE` keyed by `event_id`.

### Checkpoint behavior

The checkpoint tracks progress. Truncating the target table without replaying the source data can remove existing output without regenerating previously processed alerts.

### Schema evolution

`mergeSchema` supports compatible schema additions but should not replace explicit schema governance.

### Batch-level failures

If the target write fails, Spark may retry the micro-batch according to the streaming execution model. The write design should account for retries and duplicate-output risk.

---

# 22. Silver Telemetry SQL Verification

SQL verification provides a way to inspect persisted Silver data independently of the transformation code.

## 22.1 SQL Query

```sql
SELECT TOP (100) *
FROM [dbo].[silver_telemetry];
```

## 22.2 Purpose

This query helps inspect:

- Persisted telemetry fields.
- Vehicle identifiers and event timestamps.
- GPS and measurement values.
- Derived date fields.
- Whether the expected records are present.

The query retrieves up to 100 records. It does not guarantee a particular order because there is no `ORDER BY` clause.

## 22.3 Query Results

![Silver Telemetry Query Results](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/11_silver_telemetry_query_results.png?raw=true)

---

# 23. Rejected Records SQL Verification

The rejected-record table preserves telemetry that failed the applicable validation conditions.

## 23.1 SQL Query

```sql
SELECT TOP (100) *
FROM [dbo].[silver_rejected];
```

## 23.2 Purpose

This query can be used to inspect:

- Rejected source records.
- Available source metadata.
- The rejection reason.
- The rejection processing timestamp.
- Data quality issues requiring further investigation.

The presence of a rejected record does not necessarily mean that every field in the record is invalid. It means the record failed at least one configured rejection condition.

## 23.3 Query Results

![Silver Rejected Records Query Results](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/12_silver_rejected_query_results.png?raw=true)

---

# 24. Anomaly Records SQL Verification

The anomaly table stores individual observations identified by the configured anomaly detection rules.

## 24.1 SQL Query

```sql
SELECT TOP (100) *
FROM [dbo].[silver_telemetry_anomalies];
```

## 24.2 Purpose

The query can be used to inspect:

- Vehicle identifiers.
- Event timestamps.
- Telemetry measurements.
- Calculated movement fields.
- Individual anomaly indicators.
- Assigned anomaly types.

The anomaly table represents individual anomalous observations. It should not automatically be interpreted as one row per operational incident because multiple observations can belong to the same episode.

## 24.3 Query Results

![Silver Telemetry Anomalies Query Results](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/13_silver_telemetry_anomalies_query_results.png?raw=true)

---

# 25. Anomaly Episodes SQL Verification

The episode table summarizes groups of related anomaly observations.

## 25.1 SQL Query

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

## 25.2 Column Descriptions

| Column | Description |
|---|---|
| `vehicle_id` | Vehicle associated with the episode. |
| `anomaly_type` | Anomaly category assigned to the episode. |
| `episode_id` | Identifier assigned to the grouped episode. |
| `start_time` | First event timestamp in the episode. |
| `end_time` | Last event timestamp in the episode. |
| `reading_count` | Number of anomaly observations included. |
| `max_calculated_speed` | Maximum calculated speed in the episode. |
| `duration_seconds` | Elapsed time between the start and end of the episode. |

## 25.3 Why Aggregate Anomaly Episodes?

Aggregating anomaly observations makes it easier to analyze incidents at a higher level.

Several anomalous observations for the same vehicle and anomaly type may represent one continuing episode if the configured time-gap rule groups them together.

The episode grouping rule starts a new episode when the gap exceeds 15 minutes.

## 25.4 Query Results

![Silver Telemetry Anomaly Episodes Query Results](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/14_silver_telemetry_anomalies_episodes_query_result.png?raw=true)

---

# 26. Real-Time Alerts SQL Verification

The `realtime_fleet_alerts` table stores telemetry observations that satisfy the configured real-time alert rules.

## 26.1 Inspect Recent Alert Records

```sql
SELECT TOP (100)
    [event_id],
    [vehicle_id],
    [trip_id],
    [event_time],
    [speed_kmh],
    [engine_temp_c],
    [cargo_temp_c],
    [latitude],
    [longitude],
    [alert_type],
    [severity],
    [alert_processed_at],
    [event_date]
FROM [dbo].[realtime_fleet_alerts]
ORDER BY [alert_processed_at] DESC;
```

This query retrieves up to 100 alerts, with the most recently processed alerts appearing first.

The query assumes that the target table is exposed through the SQL endpoint using the specified schema and table name.

## 26.2 Count Alerts by Type

```sql
SELECT
    [alert_type],
    COUNT(*) AS alert_count
FROM [dbo].[realtime_fleet_alerts]
GROUP BY [alert_type]
ORDER BY alert_count DESC;
```

This query summarizes the number of stored alert rows by selected alert type.

The result counts stored rows, not necessarily unique events or unique operational incidents.

## 26.3 Count Alerts by Severity

```sql
SELECT
    [severity],
    COUNT(*) AS alert_count
FROM [dbo].[realtime_fleet_alerts]
GROUP BY [severity]
ORDER BY alert_count DESC;
```

This query summarizes the number of stored alert rows at each severity level.

## 26.4 Inspect Critical Alerts

```sql
SELECT TOP (100)
    [event_id],
    [vehicle_id],
    [event_time],
    [alert_type],
    [severity],
    [speed_kmh],
    [engine_temp_c],
    [cargo_temp_c],
    [alert_processed_at]
FROM [dbo].[realtime_fleet_alerts]
WHERE [severity] = 'CRITICAL'
ORDER BY [alert_processed_at] DESC;
```

This query isolates alerts assigned `CRITICAL` severity.

Under the implemented severity rules, a critical alert satisfies at least one of these conditions:

- Speed greater than 110 km/h.
- Engine temperature greater than 115°C.

The alert type may still be `COLD_CHAIN_VIOLATION` if the cold-chain rule has higher classification priority.

## 26.5 Exclude Critical-Speeding Alerts

To retrieve alerts whose type is not `CRITICAL_SPEEDING`:

```sql
SELECT TOP (3000) *
FROM [dbo].[realtime_fleet_alerts]
WHERE [alert_type] <> 'CRITICAL_SPEEDING';
```

This excludes rows with `alert_type` equal to `CRITICAL_SPEEDING`.

In SQL, rows with a null `alert_type` are not returned by this condition because the comparison evaluates to unknown.

## 26.6 Inspect Alert Counts by Event Date

```sql
SELECT
    [event_date],
    COUNT(*) AS alert_count
FROM [dbo].[realtime_fleet_alerts]
GROUP BY [event_date]
ORDER BY [event_date] DESC;
```

This query shows how many alert rows were stored for each event date.

It groups alerts by the original telemetry event date, not the alert processing date.

---

# 27. Anomaly Evaluation

The evaluation stage compares detected results with available ground-truth data.

The implementation reports Precision, Recall, F1-score, true positives, total detected records or groups in scope, and total ground-truth episodes.

The meaning of these metrics depends on the matching logic implemented in the evaluation workflow.

## 27.1 Evaluation SQL Query

```sql
SELECT TOP (100)
    [precision],
    [recall],
    [f1_score],
    [true_positives],
    [total_detected_in_scope],
    [total_truth_episodes]
FROM [dbo].[evaluation_metrics];
```

## 27.2 Precision

Precision measures the proportion of counted detections that were considered true positives by the evaluation logic.

\[
\text{Precision}
=
\frac{\text{True Positives}}
{\text{Total Detected}}
\]

A high precision indicates that a high proportion of counted detections matched the implemented ground-truth criteria.

## 27.3 Recall

Recall measures the proportion of ground-truth cases counted as detected.

\[
\text{Recall}
=
\frac{\text{True Positives}}
{\text{Total Ground-Truth Cases}}
\]

A high recall indicates that a larger proportion of ground-truth cases were detected.

## 27.4 F1-score

The F1-score is the harmonic mean of Precision and Recall.

\[
F_1
=
2 \times
\frac{\text{Precision}\times\text{Recall}}
{\text{Precision}+\text{Recall}}
\]

The F1-score combines precision and recall into one metric.

It is useful when both false positives and missed detections matter.

---

# 28. Evaluation Results

The recorded evaluation results are shown below.

| Metric | Result |
|---|---:|
| True positives | 5 |
| Total detected in scope | 5 |
| Total ground-truth episodes | 6 |
| Precision | 1.0000 |
| Recall | 0.8333 |
| F1-score | 0.9091 |

## 28.1 Precision Interpretation

The reported precision is 1.0000.

Under the implemented evaluation logic, all five counted detections were counted as true positives.

\[
\text{Precision} = \frac{5}{5} = 1.0
\]

## 28.2 Recall Interpretation

The reported recall is approximately 0.8333.

Five of the six ground-truth cases were counted as detected.

\[
\text{Recall} = \frac{5}{6} \approx 0.8333
\]

## 28.3 F1-score Interpretation

Using the reported precision and recall:

\[
F_1
=
2 \times
\frac{1.0 \times 0.8333}
{1.0 + 0.8333}
\approx 0.9091
\]

The F1-score summarizes the balance between the reported precision and recall.

## 28.4 Evaluation Query Results

![Anomaly Detection Evaluation Results](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/main/evidence/person2/15_evaluation_results.png?raw=true)

## 28.5 Evaluation Scope and Limitations

The metrics must be interpreted according to the implemented matching logic.

The current evaluation approach uses vehicle-level identifiers when comparing detected results with ground-truth groups. Therefore, the reported metrics should not automatically be interpreted as strict one-to-one episode-level matching metrics.

A stricter episode-level evaluation should define a matching rule based on relevant dimensions, such as:

- Vehicle identity.
- Anomaly type.
- Start and end timestamps.
- Allowed temporal overlap or timestamp tolerance.

The evaluation should then distinguish matched detections, unmatched detections, and unmatched ground-truth episodes.

This is important because matching by vehicle alone can count detections as matches without necessarily proving that the detected and ground-truth episodes represent the same incident.

The real-time alert table is a separate output and is not represented by these analytical anomaly evaluation metrics unless it is explicitly included in the evaluation workflow.

---

# 29. Data Quality Considerations

The reliability of the project depends on consistent validation and transformation rules.

## 29.1 Missing Mandatory Fields

Records without mandatory identifiers, timestamps, coordinates, or reported speed should not silently flow into calculations that require these fields.

The rejected-record table helps preserve invalid Silver records for investigation.

The real-time alerting pipeline applies its own required-field filter and does not automatically send filtered-out records to the Silver rejected table.

## 29.2 Timestamp Quality

Movement calculations depend on event ordering and timestamp differences.

Missing timestamps, duplicate timestamps, and irregular sampling intervals can affect speed calculations and episode boundaries.

## 29.3 GPS Accuracy

GPS coordinates can contain measurement errors, jumps, or other inconsistencies.

Distance and speed calculations should therefore be interpreted together with the movement filtering rules.

## 29.4 Duplicate Records

Micro-batch deduplication and existing-table checks serve different purposes.

The Silver pipeline uses `db_id` to help prevent repeated source identifiers from being appended again. Business-key deduplication within a batch handles another class of duplicate observations.

The real-time alerting pipeline appends alerts without an explicit target-level deduplication step.

These checks do not automatically guarantee uniqueness across every possible replay, concurrent execution, or source update scenario.

## 29.5 Rejected Record Monitoring

The rejected table can help identify recurring source problems.

Useful follow-up analyses include counting rejected records by rejection reason, examining missing fields, and tracking rejection volume over time.

## 29.6 Analytical Anomaly Thresholds

| Rule | Threshold |
|---|---|
| Engine overheating | Engine temperature greater than 105°C |
| GPS teleportation | Calculated speed greater than 100 km/h and distance greater than 0.5 km |
| RPM spike | RPM greater than 4500 |
| Analytical cold-chain breach | Cargo temperature below -2°C or above 12°C |
| Excessive idling | Reported speed below 2 km/h and RPM greater than zero |

## 29.7 Real-Time Alert Thresholds

| Alert rule | Threshold |
|---|---|
| Cold-chain violation | Cargo temperature below 2°C or above 8°C |
| Engine overheating | Engine temperature greater than 105°C |
| Critical-speeding alert | Reported speed greater than 105 km/h |

## 29.8 Alert Severity Thresholds

| Severity | Threshold |
|---|---|
| `CRITICAL` | Speed greater than 110 km/h or engine temperature greater than 115°C |
| `HIGH` | Speed greater than 105 km/h when the critical condition is not met |
| `MEDIUM` | Remaining generated alerts |

The real-time alert thresholds and severity rules are specific to the current implementation. They should be reviewed against actual operating requirements before using the rules in a production monitoring system.

---

# 30. Limitations and Improvement Opportunities

The current implementation demonstrates the major processing stages, but several areas require attention when moving toward a more robust production design.

## 30.1 Cross-Micro-Batch Deduplication

`dropDuplicates(["db_id"])` removes duplicate identifiers within the dataframe on which it is executed.

It does not, on its own, guarantee cross-micro-batch uniqueness.

The existing Silver-table anti join checks previously persisted identifiers, but its effectiveness depends on the execution strategy, table state, and retry behavior.

A more robust implementation should explicitly define how duplicate writes and concurrent executions are handled.

## 30.2 Business-Key Uniqueness

Deduplicating on `vehicle_id` and `event_time` within an incoming batch does not automatically remove duplicates that already exist in the target table under a different `db_id`.

The business key should be enforced consistently if it represents the intended definition of a unique observation.

## 30.3 Movement Speed Filtering

The movement-processing logic sets calculated speeds greater than 180 km/h to zero.

The GPS anomaly rule then checks for calculated speeds greater than 100 km/h and distances greater than 0.5 km.

Because values above 180 km/h are reset before the downstream rule runs, the anomaly detector may not see their original speed values.

If the intention is to preserve these events for anomaly detection, the original calculated speed should be retained separately from the filtered analytical speed.

## 30.4 Episode-Level Evaluation

The evaluation results depend on the implemented matching criteria.

Strict episode-level evaluation would provide stronger evidence by matching detections to ground-truth episodes using vehicle identity, anomaly type, and temporal overlap.

## 30.5 Streaming Retry Behavior

The Silver pipeline writes rejected and valid records to separate Delta tables inside a `foreachBatch` function.

If a failure occurs between these writes, a retry can repeat one write while the other has not completed.

Production implementations should consider idempotent writes, batch identifiers, transactional design, and recovery testing.

## 30.6 Real-Time Alert Idempotency

The real-time alerting pipeline uses append mode:

```python
df_alerts.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("event_date") \
    .saveAsTable("realtime_fleet_alerts")
```

Append mode does not automatically enforce uniqueness of `event_id`.

If the same source event is processed more than once due to replay, checkpoint changes, or recovery behavior, duplicate alert rows may be produced.

Potential improvements include:

- Defining an alert business key.
- Preserving `db_id` or another stable event identifier in the alert output.
- Using an idempotent write strategy.
- Implementing a Delta `MERGE` based on a defined key where appropriate.
- Testing recovery and replay scenarios.
- Separating full-refresh behavior from normal incremental execution.

The exact approach depends on the expected source semantics and the desired behavior when an event is updated.

## 30.7 Target Truncation and Checkpoint Coordination

Truncating `realtime_fleet_alerts` while retaining the checkpoint can create an inconsistency between target data and recorded source progress.

The target may be empty even though the checkpoint indicates that source data has already been processed.

A safer design should distinguish between:

- Normal incremental processing, where existing alert data is preserved.
- A deliberate full refresh, where target reset and source replay are coordinated.

Checkpoint deletion or reset should not be performed casually because it can trigger replay and duplicate output.

## 30.8 Available-Now Versus Continuous Streaming

The current real-time query uses:

```python
.trigger(availableNow=True)
```

This processes currently available source data and then terminates.

It is suitable for bounded incremental processing but does not remain continuously active for future arrivals after the current run completes.

For continuous operational alerting, a long-running streaming deployment and an appropriate trigger strategy are needed.

## 30.9 Alert Classification and Severity

The `alert_type` priority is:

1. `COLD_CHAIN_VIOLATION`
2. `ENGINE_OVERHEATING`
3. `CRITICAL_SPEEDING`

Severity is calculated separately using speed and engine-temperature conditions.

As a result, the name `CRITICAL_SPEEDING` does not guarantee that the assigned `severity` is `CRITICAL`.

If the business requirement defines severity based on alert type, the classification logic should explicitly encode that relationship.

## 30.10 Cold-Chain Threshold Consistency

The analytical cold-chain rule uses -2°C and 12°C, while the real-time alerting rule uses 2°C and 8°C.

This can result in the two pipelines classifying the same temperature differently.

The threshold differences should be documented and reviewed with the operational requirements. If both pipelines should apply the same policy, they should use a shared configuration or clearly aligned thresholds.

## 30.11 Schema Evolution

The use of `mergeSchema` can allow compatible schema changes during writes.

However, schema evolution should be managed deliberately so that unexpected source changes do not silently create inconsistent downstream datasets.

## 30.12 Monitoring and Observability

Additional monitoring can track:

- Number of incoming records.
- Number of valid records.
- Number of rejected records.
- Duplicate counts.
- Processing duration.
- Number of detected anomalies.
- Number of aggregated episodes.
- Number of real-time alerts.
- Alert counts by type and severity.
- Streaming query failures.
- Evaluation metric changes over time.

These measurements can help identify data quality problems and unexpected changes in pipeline behavior.

---

# 31. Final Summary

The Fleet Telematics Lakehouse project demonstrates telemetry processing, data quality enforcement, movement analytics, anomaly detection, episode aggregation, and real-time alert generation.

The Silver-layer workflow includes:

- Structured schema parsing.
- Mandatory-field validation.
- Rejected-record handling.
- Micro-batch deduplication.
- Business-key deduplication within the processed dataframe.
- Incremental duplicate prevention using `db_id`.
- Date-based partitioning.
- Reference enrichment using a broadcast join.
- Vehicle movement analytics using window functions.
- GPS-based distance and calculated-speed logic.
- Idling and cold-chain indicators.
- Rule-based anomaly detection.
- Anomaly episode aggregation.
- Incremental ingestion with Spark Structured Streaming.
- SQL verification of persisted datasets.
- Evaluation using Precision, Recall, and F1-score.

The real-time alerting workflow includes:

- Reading `bronze_telemetry_v2` as a Delta stream.
- Parsing telemetry JSON using an explicit schema.
- Filtering records to `source == "kafka_cdc"`.
- Validating event identifiers, vehicle identifiers, and timestamps.
- Detecting cold-chain violations.
- Detecting engine overheating.
- Detecting speeding above the configured threshold.
- Assigning an alert type according to a priority order.
- Assigning `CRITICAL`, `HIGH`, or `MEDIUM` severity.
- Adding processing timestamps and an event-date partitioning column.
- Appending alerts to `realtime_fleet_alerts`.
- Validating alert records using SQL.

The recorded analytical evaluation results are:

| Metric | Result |
|---|---:|
| Precision | 1.0000 |
| Recall | 0.8333 |
| F1-score | 0.9091 |
| True positives | 5 |
| Total detected in scope | 5 |
| Total ground-truth episodes | 6 |

These results should be understood within the scope and limitations of the current evaluation logic.

The real-time alerting pipeline is a separate operational monitoring workflow and should not be assumed to be covered by the analytical evaluation metrics.

Overall, the project demonstrates an end-to-end fleet telemetry processing workflow that combines curated data preparation, incremental ingestion, telemetry enrichment, movement analytics, anomaly detection, episode aggregation, and real-time operational alerts.
