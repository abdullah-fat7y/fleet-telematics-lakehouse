
# Fleet Telematics Lakehouse
## Person 2 — Silver Processing, Telemetry Analytics, and Anomaly Detection

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
- [20. Incremental Streaming Pipeline](#20-incremental-streaming-pipeline)
- [21. Silver Telemetry SQL Verification](#21-silver-telemetry-sql-verification)
- [22. Rejected Records SQL Verification](#22-rejected-records-sql-verification)
- [23. Anomaly Records SQL Verification](#23-anomaly-records-sql-verification)
- [24. Anomaly Episodes SQL Verification](#24-anomaly-episodes-sql-verification)
- [25. Anomaly Evaluation](#25-anomaly-evaluation)
- [26. Evaluation Results](#26-evaluation-results)
- [27. Data Quality Considerations](#27-data-quality-considerations)
- [28. Limitations and Improvement Opportunities](#28-limitations-and-improvement-opportunities)
- [29. Final Summary](#29-final-summary)

---

# 1. Project Overview

The Fleet Telematics Lakehouse project processes vehicle telemetry data to support vehicle monitoring, movement analytics, cold-chain monitoring, and anomaly detection.

The system receives telemetry records containing vehicle identifiers, timestamps, GPS coordinates, speed, engine measurements, fuel information, and cargo temperature.

The data is processed through a Medallion Architecture consisting of Bronze, Silver, and Gold layers.

The Bronze layer stores ingested data, while the Silver layer applies data quality rules, transformations, deduplication, enrichment, and analytical calculations.

The processed data can then be used by downstream analytical workloads and reporting solutions.

The main focus of this document is the **Silver layer**, including its transformation logic, incremental ingestion, anomaly detection, anomaly episode aggregation, SQL validation, and evaluation.

## Main Objectives

- Parse incoming telemetry records into a structured schema.
- Validate mandatory telemetry fields.
- Separate invalid records from valid records.
- Prevent duplicate records from entering the curated dataset.
- Store cleaned telemetry in a Delta table.
- Organize the Silver data using date-based partitioning.
- Enrich telemetry with vehicle reference information.
- Calculate vehicle movement metrics from GPS coordinates.
- Detect excessive idling and cold-chain violations.
- Detect GPS-related and engine-related anomalies.
- Group anomaly observations into episodes.
- Process newly available records incrementally.
- Validate persisted tables using SQL queries.
- Evaluate anomaly detection results against ground-truth data.

---

# 2. Scope of Work

The Silver processing workflow includes the following responsibilities.

| Area | Description |
|---|---|
| Schema parsing | Convert raw JSON telemetry into structured columns. |
| Data validation | Identify records with missing mandatory values or invalid source status. |
| Rejected records | Preserve rejected telemetry for inspection. |
| Deduplication | Remove repeated records according to configured keys. |
| Incremental processing | Process newly available records and avoid re-ingesting existing identifiers. |
| Delta Lake | Persist the curated telemetry and analytical datasets. |
| Partitioning | Organize Silver telemetry by event date. |
| Enrichment | Add reference attributes through a vehicle-based join. |
| Movement analytics | Calculate distance, time gaps, and estimated speed. |
| Rule-based detection | Identify configured operational anomalies. |
| Episode aggregation | Group related anomalies by vehicle, type, and time gap. |
| SQL verification | Inspect the resulting tables. |
| Evaluation | Calculate Precision, Recall, and F1-score using the implemented evaluation logic. |

---

# 3. Technology Stack

The implementation uses technologies and concepts associated with distributed data processing and lakehouse engineering.

| Technology or Concept | Purpose |
|---|---|
| Apache Spark | Distributed data processing. |
| PySpark | Python API for Spark transformations. |
| Spark SQL | Querying processed datasets. |
| Spark Structured Streaming | Incremental processing of incoming data. |
| Delta Lake | Table storage and transactional data operations. |
| Window Functions | Comparing observations belonging to the same vehicle. |
| Broadcast Join | Enriching telemetry with a sufficiently small reference dataset. |
| SQL | Validating the output tables. |
| Precision, Recall, F1-score | Evaluating anomaly detection performance. |

---

# 4. Processing Architecture

The logical Silver processing workflow is:

```text
                    Bronze Layer
                          |
                          v
                bronze_telemetry_v2
                          |
                          v
              Read and Parse Telemetry
                          |
                          v
                 Data Validation
                          |
                 +--------+--------+
                 |                 |
                 v                 v
           Valid Records     Rejected Records
                 |                 |
                 v                 v
          Deduplication      silver_rejected
                 |
                 v
          silver_telemetry
                 |
                 v
         Vehicle Enrichment
                 |
                 v
        Vehicle Movement Metrics
                 |
                 v
          Anomaly Detection
                 |
                 v
      silver_telemetry_anomalies
                 |
                 v
        Anomaly Episode Grouping
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

The main persisted datasets used in the workflow include:

- `bronze_telemetry_v2`
- `silver_telemetry`
- `silver_rejected`
- `silver_telemetry_anomalies`
- `silver_anomaly_episodes`
- `evaluation_metrics`

The availability and exact schema of each table depend on the executed pipeline and the environment in which the tables were created.

---

# 5. Bronze Layer Inspection

Before applying transformations, the source data should be inspected to understand its structure and identify the fields required by downstream processing.

The Bronze table is the starting point for the Silver transformation.

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

## 5.3 Bronze Sample Data

![Bronze Sample Data](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/01_bronze_sample_data.png?raw=true)

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

The processing rules are applied before the resulting data is used for movement calculations and anomaly detection.

## 6.1 Processing Requirements

The pipeline needs to distinguish between two types of problems.

**Structural problems** occur when a record cannot be parsed correctly or expected fields are missing.

**Data quality problems** occur when a record is parsed but does not satisfy the required validation conditions.

Separating these cases makes the processing output easier to inspect and troubleshoot.

## 6.2 Bronze-to-Silver Processing Evidence

![Bronze-to-Silver Incremental Data Processing and Schema Parsing](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/02_Bronze-to-Silver%20Incremental%20data%20Processing%20and%20Schema%20Parsing.png?raw=true)

---

# 7. Schema Parsing

The streaming implementation defines an explicit schema for the telemetry JSON payload.

An explicit schema helps Spark interpret the source fields without relying on inference for every micro-batch.

## 7.1 Define the Telemetry Schema

```python
from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
    BooleanType
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

## 7.3 Convert the Event Timestamp

The schema defines `event_time` as a string because the source JSON timestamp is initially represented as text.

The pipeline converts it to a timestamp:

```python
df_parsed = df_parsed.withColumn(
    "event_time",
    F.col("event_time").cast(TimestampType())
)
```

The converted timestamp is required for chronological ordering, movement calculations, date partitioning, and anomaly episode grouping.

## 7.4 Schema Parsing Evidence

![Bronze-to-Silver Incremental Data Processing and Schema Parsing](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/02_Bronze-to-Silver%20Incremental%20data%20Processing%20and%20Schema%20Parsing.png?raw=true)

---

# 8. Data Validation

Data validation identifies records that do not contain the mandatory values required for downstream processing.

For the streaming Silver workflow, the mandatory fields include:

- `event_id`
- `vehicle_id`
- `event_time`
- `latitude`
- `longitude`
- `speed_kmh`

These fields are necessary for identifying the event, associating it with a vehicle, ordering it in time, locating it geographically, and evaluating movement.

## 8.1 Validation Condition

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

## 8.2 Why Validate Before Processing?

Missing mandatory values can affect later calculations.

For example:

- A missing vehicle identifier prevents reliable vehicle-level grouping.
- A missing event timestamp prevents reliable chronological ordering.
- Missing GPS coordinates prevent distance calculation.
- Missing speed prevents the configured reported-speed checks.

Validation helps isolate these records instead of allowing them to flow silently into the curated dataset.

## 8.3 Validation Evidence

![Data Validation, Deduplication, and Rejected Records Handling](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/03_Data%20Validation%2C%20Deduplication%2C%20and%20Rejected%20Records%20Handling.png?raw=true)

---

# 9. Rejected Records Handling

Invalid telemetry records should be preserved separately when the pipeline needs to support data quality investigation.

The streaming implementation considers both the source validity flag and the required-field validation condition.

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

This provides a reason for inspection and records the time when the rejected record was processed.

## 9.3 Write Rejected Records

```python
if not df_rejected.isEmpty():
    df_rejected.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("silver_rejected")
```

The rejected records are appended to the `silver_rejected` table when the micro-batch contains rejected data.

The `mergeSchema` option allows compatible schema additions during writes, but it does not replace the need for deliberate schema management.

## 9.4 Benefits of a Rejected Records Table

- Invalid data remains available for investigation.
- Rejection reasons can be inspected.
- Data quality problems can be quantified.
- Invalid records can be separated from analytical data.
- Source issues can be investigated without contaminating the valid Silver dataset.

## 9.5 Rejected Records Evidence

![Data Validation, Deduplication, and Rejected Records Handling](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/03_Data%20Validation%2C%20Deduplication%2C%20and%20Rejected%20Records%20Handling.png?raw=true)

---

# 10. Deduplication

Deduplication prevents repeated observations from unnecessarily increasing record counts or distorting downstream calculations.

The implementation uses more than one deduplication rule because duplicate detection can occur at different stages.

## 10.1 Micro-Batch Deduplication

The streaming micro-batch removes duplicate source records using `db_id`.

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

The keys represent different concepts.

| Key | Purpose |
|---|---|
| `db_id` | Identifies a source record for ingestion-level deduplication. |
| `vehicle_id`, `event_time` | Identifies a telemetry observation according to the configured business rule. |

The correct key depends on the source system and the intended definition of a unique telemetry event.

## 10.4 Deduplication Evidence

![Silver Layer Deduplication and Date-Based Partitioning](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/04_Silver%20Layer%20Deduplication%20and%20Date-Based%20Partitioning.png?raw=true)

---

# 11. Incremental Processing and Duplicate Prevention

Incremental ingestion processes newly available records without blindly appending every record from the source.

The implementation checks whether incoming valid records already exist in the Silver dataset.

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

This helps avoid re-inserting source identifiers already present in the table.

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

This write appends the newly identified records to `silver_telemetry`.

## 11.5 Important Implementation Detail

The existing-table anti join prevents repeated `db_id` values from being inserted when the check is applied successfully.

However, the check is based on `db_id`. It does not independently guarantee that a business key such as `(vehicle_id, event_time)` is unique across all previously written data.

Similarly, `dropDuplicates` inside a micro-batch does not automatically deduplicate records across different micro-batches.

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

The event date is derived from the event timestamp.

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

![Silver Layer Deduplication and Date-Based Partitioning](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/04_Silver%20Layer%20Deduplication%20and%20Date-Based%20Partitioning.png?raw=true)

---

# 13. Telemetry Enrichment

Telemetry enrichment adds contextual information to the processed vehicle observations.

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

A left join retains the telemetry records even if a matching reference record is not found.

This can be useful when missing reference data should not automatically discard otherwise valid telemetry.

## 13.3 Why Use a Broadcast Join?

Broadcast joins are useful when one side of the join is sufficiently small.

Spark can distribute the smaller reference dataset to the executors, potentially avoiding a large shuffle of the telemetry data.

Broadcasting an oversized dataset can increase memory pressure, so the suitability of the join depends on the actual reference table size.

## 13.4 Enrichment Evidence

![Silver Telemetry Enrichment Using Broadcast Join](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/05_Silver%20Telemetry%20Enrichment%20Using%20Broadcast%20Join.png?raw=true)

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

This ensures that previous values are retrieved from the same vehicle's ordered observations.

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

This value is used to determine whether consecutive observations are suitable for calculating movement speed.

## 14.4 Why Use Window Functions?

Window functions allow calculations across related rows without collapsing the dataset into one aggregate row per vehicle.

They are useful for:

- Retrieving previous observations.
- Calculating time differences.
- Comparing current and previous GPS coordinates.
- Deriving movement metrics.
- Identifying changes between consecutive telemetry events.

## 14.5 Movement Analytics Evidence

![Vehicle Movement Analytics and Cold Chain Violation Detection Using PySpark](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/06_Vehicle%20Movement%20Analytics%20and%20Cold%20Chain%20Violation%20Detection%20Using%20PySpark.png?raw=true)

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

The distance calculation compares the current GPS position with the preceding position for the same vehicle.

## 15.2 Distance Calculation Considerations

Distance calculations depend on the availability of both the current and previous coordinates.

The first observation for a vehicle cannot be compared with a preceding observation using this window.

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

The calculation should only be applied when the time gap and distance satisfy the configured processing conditions.

In the implemented movement logic, observations are eligible for calculation when:

- The time gap is greater than 3 seconds.
- The time gap is no greater than 3600 seconds.
- The distance is at least 0.01 km.

## 15.4 Speed Filtering

The processing logic sets calculated speeds greater than 180 km/h to zero.

This is an implementation-specific filtering rule and should be considered when interpreting downstream anomaly detection.

In particular, values above the filtering threshold may no longer retain their original calculated speed after this transformation.

## 15.5 Why Calculate Speed from GPS?

Reported vehicle speed and speed calculated from GPS are different measurements.

Calculated speed can be used to compare consecutive positions with the reported telemetry and to identify suspicious movement patterns.

However, GPS-derived speed is sensitive to position accuracy and event timing. It should therefore be interpreted alongside the validation and filtering rules applied by the pipeline.

---

# 16. Idling and Cold-Chain Detection

The movement analytics stage also derives operational indicators that are useful for vehicle monitoring.

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

## 16.2 Cold-Chain Violation

The configured cold-chain condition is:

```python
is_cold_chain_violation = (
    (F.col("cargo_temp_c") < -2)
    | (F.col("cargo_temp_c") > 12)
)
```

The condition identifies cargo-temperature readings below -2°C or above 12°C.

The configured range is specific to this project and should not be treated as a universal temperature requirement for every cargo type.

## 16.3 Operational Use

These indicators can help identify:

- Potential excessive idling.
- Cargo-temperature excursions.
- Vehicle operating conditions requiring further inspection.
- Observations that may need to be grouped into longer anomaly episodes.

## 16.4 Cold-Chain and Movement Evidence

![Vehicle Movement Analytics and Cold Chain Violation Detection Using PySpark](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/06_Vehicle%20Movement%20Analytics%20and%20Cold%20Chain%20Violation%20Detection%20Using%20PySpark.png?raw=true)

---

# 17. Anomaly Detection Rules

The anomaly detection stage evaluates telemetry observations against the project's configured operational rules.

The output records retain the relevant telemetry and calculated fields alongside anomaly indicators.

## 17.1 Engine Overheating

The engine overheating condition is:

```python
engine_overheating_condition = (
    F.col("engine_temp_c") > 105
)
```

The condition is true when engine temperature exceeds 105°C.

## 17.2 GPS Teleportation

The configured GPS-related condition is:

```python
gps_teleportation_condition = (
    (F.col("calculated_speed_kmh") > 100)
    & (F.col("distance_km") > 0.5)
)
```

The condition identifies an observation with calculated speed above 100 km/h and distance greater than 0.5 km.

The rule is intended to identify suspicious movement observations. It is not, by itself, proof of GPS spoofing or a real-world vehicle teleportation event.

## 17.3 RPM Spike

The RPM spike condition is:

```python
rpm_spike_condition = (
    F.col("rpm") > 4500
)
```

The condition is true when RPM exceeds 4500.

## 17.4 Cold-Chain Breach

The cold-chain breach uses the existing cold-chain indicator:

```python
cold_chain_breach_condition = (
    F.col("is_cold_chain_violation")
)
```

## 17.5 Excessive Idling

The excessive-idling anomaly uses the existing idling indicator:

```python
excessive_idling_condition = (
    F.col("is_idling")
)
```

## 17.6 Combined Anomaly Indicator

The overall anomaly indicator is based on the configured individual flags.

Conceptually:

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

![Anomaly Detection and Episode Aggregation for Vehicle Telemetry](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/07_Anomaly%20Detection%20and%20Episode%20Aggregation%20for%20Vehicle%20Telemetry.png?raw=true)

---

# 18. Anomaly Type Assignment

An observation can satisfy more than one anomaly rule.

When a single anomaly category is assigned, the configured priority order determines which category is selected.

The implementation uses the following priority:

1. `cold_chain_breach`
2. `excessive_idling`
3. `route_deviation`
4. `engine_overheating`
5. `rpm_spike`

This ordering allows the processing logic to assign a single category according to the priority rules.

## 18.1 Priority-Based Classification

Conceptually, a priority-based classification can be expressed as:

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

This window ensures that the previous event is calculated within the same vehicle and anomaly category.

## 19.3 Retrieve the Previous Event Time

```python
df_episodes = df_anomalies.withColumn(
    "prev_event_time",
    F.lag("event_time").over(episode_window)
)
```

The first event in each partition has a null previous timestamp.

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

The difference is converted to minutes.

## 19.5 Identify Episode Boundaries

The configured episode rule starts a new episode when the previous event does not exist or the time gap is greater than 15 minutes.

Conceptually:

```python
new_episode_condition = (
    F.col("prev_event_time").isNull()
    | (F.col("gap_minutes") > 15)
)
```

A cumulative sum of the new-episode indicator can then assign an episode identifier within each partition.

## 19.6 Aggregate Episode Metrics

The episode aggregation summarizes related anomaly observations.

Typical metrics include:

- Episode start time.
- Episode end time.
- Number of readings.
- Maximum calculated speed.
- Duration in seconds.

Conceptually:

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

The duration can be calculated from the difference between the start and end timestamps.

The actual implementation must ensure that the episode identifier is generated before grouping and that its grouping keys uniquely identify an episode within the intended scope.

## 19.7 Episode Duration

The episode duration is based on the first and last timestamps:

```text
duration_seconds =
    end_time - start_time
```

A single-reading episode may have a duration of zero when the start and end timestamps are equal.

## 19.8 Episode Aggregation Evidence

![Anomaly Detection and Episode Aggregation for Vehicle Telemetry](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/07_Anomaly%20Detection%20and%20Episode%20Aggregation%20for%20Vehicle%20Telemetry.png?raw=true)

---

# 20. Incremental Streaming Pipeline

The implementation uses Spark Structured Streaming to process records from the Bronze Delta table.

The pipeline processes each micro-batch using a `foreachBatch` function.

## 20.1 Import Required Functions

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
    BooleanType
)
```

## 20.2 Read the Bronze Stream

```python
df_bronze_stream = spark.readStream \
    .format("delta") \
    .option("ignoreChanges", "true") \
    .table("bronze_telemetry_v2")
```

This creates a streaming DataFrame from the Bronze Delta table.

The `ignoreChanges` option affects how updates to the source Delta table are handled. It should not be interpreted as a guarantee that every changed source row will be reprocessed as a new event.

## 20.3 Process Each Micro-Batch

The micro-batch function implements the parsing, validation, rejection handling, deduplication, and incremental write logic.

```python
def process_silver_streaming_microbatch(
    microBatchDF,
    batchId
):
    if microBatchDF.isEmpty():
        return

    df_parsed = microBatchDF.withColumn(
        "parsed_data",
        F.from_json(
            F.col("raw"),
            telemetry_schema
        )
    ).select(
        F.col("db_id"),
        F.col("source"),
        F.col("is_valid"),
        F.col("reject_reason"),
        F.col("EventEnqueuedUtcTime"),
        F.col("parsed_data.*")
    ).withColumn(
        "event_time",
        F.col("event_time").cast(TimestampType())
    )

    validation_condition = (
        F.col("event_id").isNotNull()
        & F.col("vehicle_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("speed_kmh").isNotNull()
    )

    df_deduplicated_mb = df_parsed.dropDuplicates(
        ["db_id"]
    )

    df_rejected = df_deduplicated_mb.filter(
        (F.col("is_valid") == False)
        | (~validation_condition)
    ).withColumn(
        "rejection_reason",
        F.coalesce(
            F.col("reject_reason"),
            F.lit(
                "missing mandatory fields "
                "(null keys/coords/speed)"
            )
        )
    ).withColumn(
        "rejected_at",
        F.current_timestamp()
    )

    if not df_rejected.isEmpty():
        df_rejected.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("silver_rejected")

    df_valid = df_deduplicated_mb.filter(
        (F.col("is_valid") == True)
        & validation_condition
    )

    df_valid_clean = df_valid.dropDuplicates(
        ["vehicle_id", "event_time"]
    ).withColumn(
        "event_date",
        F.to_date(F.col("event_time"))
    )

    if spark.catalog.tableExists("silver_telemetry"):
        silver_existing = spark.read.table(
            "silver_telemetry"
        ).select("db_id")

        new_valid_records = df_valid_clean.join(
            silver_existing,
            on="db_id",
            how="left_anti"
        )
    else:
        new_valid_records = df_valid_clean

    if not new_valid_records.isEmpty():
        new_valid_records.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("event_date") \
            .saveAsTable("silver_telemetry")
```

## 20.4 Start the Streaming Query

```python
query = df_bronze_stream.writeStream \
    .format("delta") \
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

print("write new stream only")
```

## 20.5 Processing Sequence

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

## 20.6 Streaming Considerations

- The checkpoint location must be accessible to the Spark execution environment.
- The source table must support the configured streaming read.
- The target tables must exist or be creatable in the execution environment.
- The schema used by `from_json` must match the incoming payload.
- Duplicate prevention should be tested across repeated runs and across micro-batches.
- The append operations should be designed with retry behavior in mind.
- `availableNow=True` processes currently available input and terminates rather than remaining active indefinitely.

## 20.7 Streaming Pipeline Evidence

![Incremental Streaming Ingestion with Duplicate Prevention in the Silver Layer](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/10_Incremental%20Streaming%20Ingestion%20with%20Duplicate%20Prevention%20in%20the%20Silver%20Layer.png?raw=true)

---

# 21. Silver Telemetry SQL Verification

SQL verification provides a way to inspect the persisted Silver data independently of the transformation code.

The following query retrieves a sample of the Silver telemetry records.

## 21.1 SQL Query

```sql
SELECT TOP (100) *
FROM [dbo].[silver_telemetry];
```

## 21.2 Purpose

This query helps inspect:

- The persisted telemetry fields.
- Vehicle identifiers and event timestamps.
- GPS and measurement values.
- Derived date fields.
- Whether the expected records are present.

The query retrieves up to 100 records. It does not guarantee that the rows are returned in a particular order because there is no `ORDER BY` clause.

## 21.3 Query Results

![Silver Telemetry Query Results](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/11_silver_telemetry_query_results.png?raw=true)

---

# 22. Rejected Records SQL Verification

The rejected-record table preserves telemetry that failed the applicable validation conditions.

## 22.1 SQL Query

```sql
SELECT TOP (100) *
FROM [dbo].[silver_rejected];
```

## 22.2 Purpose

This query can be used to inspect:

- Rejected source records.
- Available source metadata.
- The rejection reason.
- The rejection processing timestamp.
- Data quality issues requiring further investigation.

The presence of a rejected record does not necessarily mean that every field in the record is invalid. It means the record failed at least one of the configured rejection conditions.

## 22.3 Query Results

![Silver Rejected Records Query Results](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/12_silver_rejected_query_results.png?raw=true)

---

# 23. Anomaly Records SQL Verification

The anomaly table stores the individual observations identified by the configured anomaly detection rules.

## 23.1 SQL Query

```sql
SELECT TOP (100) *
FROM [dbo].[silver_telemetry_anomalies];
```

## 23.2 Purpose

The query can be used to inspect:

- Vehicle identifiers.
- Event timestamps.
- Telemetry measurements.
- Calculated movement fields.
- Individual anomaly indicators.
- Assigned anomaly types.

The anomaly table represents individual anomalous observations. It should not automatically be interpreted as one row per operational incident because multiple observations can belong to the same episode.

## 23.3 Query Results

![Silver Telemetry Anomalies Query Results](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/13_silver_telemetry_anomalies_query_results.png?raw=true)

---

# 24. Anomaly Episodes SQL Verification

The episode table summarizes groups of related anomaly observations.

## 24.1 SQL Query

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

## 24.2 Column Descriptions

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

## 24.3 Why Aggregate Anomaly Episodes?

Aggregating anomaly observations makes it easier to analyze incidents at a higher level.

For example, several anomalous observations for the same vehicle and anomaly type may represent one continuing episode if the configured time-gap rule groups them together.

The episode grouping rule used by this implementation starts a new episode when the gap exceeds 15 minutes.

## 24.4 Query Results

![Silver Telemetry Anomaly Episodes Query Results](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/14_silver_telemetry_anomalies_episodes_query_result.png?raw=true)

---

# 25. Anomaly Evaluation

The evaluation stage compares the detected results with the available ground-truth data.

The implementation reports Precision, Recall, F1-score, true positives, total detected records or groups in scope, and the total number of ground-truth episodes.

The meaning of these metrics depends on the matching logic implemented in the evaluation workflow.

## 25.1 Evaluation SQL Query

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

## 25.2 Precision

Precision measures the proportion of counted detections that were considered true positives by the evaluation logic.

\[
\text{Precision}
=
\frac{\text{True Positives}}
{\text{Total Detected}}
\]

A high precision indicates that a high proportion of counted detections matched the implemented ground-truth criteria.

## 25.3 Recall

Recall measures the proportion of ground-truth cases counted as detected.

\[
\text{Recall}
=
\frac{\text{True Positives}}
{\text{Total Ground-Truth Cases}}
\]

A high recall indicates that a larger proportion of the ground-truth cases were detected.

## 25.4 F1-score

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

# 26. Evaluation Results

The recorded evaluation results are shown below.

| Metric | Result |
|---|---:|
| True positives | 5 |
| Total detected in scope | 5 |
| Total ground-truth episodes | 6 |
| Precision | 1.0000 |
| Recall | 0.8333 |
| F1-score | 0.9091 |

## 26.1 Precision Interpretation

The reported precision is 1.0000.

Under the implemented evaluation logic, all five counted detections were counted as true positives.

\[
\text{Precision} = \frac{5}{5} = 1.0
\]

## 26.2 Recall Interpretation

The reported recall is approximately 0.8333.

Five of the six ground-truth cases were counted as detected.

\[
\text{Recall} = \frac{5}{6} \approx 0.8333
\]

## 26.3 F1-score Interpretation

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

## 26.4 Evaluation Query Results

![Anomaly Detection Evaluation Results](https://github.com/abdullah-fat7y/fleet-telematics-lakehouse/blob/feat/silver_processing/evidence/person2/15_evaluation_results.png?raw=true)

## 26.5 Evaluation Scope and Limitations

The metrics must be interpreted according to the implemented matching logic.

The current evaluation approach uses vehicle-level identifiers when comparing detected results with ground-truth groups. Therefore, the reported metrics should not automatically be interpreted as strict one-to-one episode-level matching metrics.

A stricter episode-level evaluation should define a matching rule based on the relevant dimensions, such as:

- Vehicle identity.
- Anomaly type.
- Start and end timestamps.
- Allowed temporal overlap or timestamp tolerance.

The evaluation should then distinguish matched detections, unmatched detections, and unmatched ground-truth episodes.

This is important because matching by vehicle alone can count detections as matches without necessarily proving that the detected and ground-truth episodes represent the same incident.

---

# 27. Data Quality Considerations

The reliability of the Silver layer depends on consistent validation and transformation rules.

## 27.1 Missing Mandatory Fields

Records without the mandatory identifiers, timestamp, coordinates, or reported speed should not silently flow into calculations that require these fields.

The rejected-record table helps preserve these records for investigation.

## 27.2 Timestamp Quality

Movement calculations depend on event ordering and timestamp differences.

Missing timestamps, duplicate timestamps, and irregular sampling intervals can affect speed calculations and episode boundaries.

## 27.3 GPS Accuracy

GPS coordinates can contain measurement errors, jumps, or other inconsistencies.

Distance and speed calculations should therefore be interpreted together with the movement filtering rules.

## 27.4 Duplicate Records

Micro-batch deduplication and existing-table checks serve different purposes.

The pipeline's use of `db_id` helps prevent repeated source identifiers from being appended again. Business-key deduplication within a batch handles another class of duplicate observations.

These checks do not automatically guarantee uniqueness across every possible replay, concurrent execution, or source update scenario.

## 27.5 Rejected Record Monitoring

The rejected table can help identify recurring source problems.

Useful follow-up analyses include counting rejected records by rejection reason, examining missing fields, and tracking rejection volume over time.

## 27.6 Anomaly Thresholds

The configured anomaly rules are project-specific:

| Rule | Threshold |
|---|---|
| Engine overheating | Engine temperature greater than 105°C |
| GPS teleportation | Calculated speed greater than 100 km/h and distance greater than 0.5 km |
| RPM spike | RPM greater than 4500 |
| Cold-chain breach | Cargo temperature below -2°C or above 12°C |
| Excessive idling | Reported speed below 2 km/h and RPM greater than zero |

These thresholds should be reviewed against actual operating requirements before using the rules in a production monitoring system.

---

# 28. Limitations and Improvement Opportunities

The current implementation demonstrates the major processing stages, but several areas require attention when moving toward a more robust production design.

## 28.1 Cross-Micro-Batch Deduplication

`dropDuplicates(["db_id"])` removes duplicate identifiers within the dataframe on which it is executed.

It does not, on its own, guarantee cross-micro-batch uniqueness.

The existing Silver-table anti join checks previously persisted identifiers, but its effectiveness depends on the execution strategy, table state, and retry behavior.

A more robust implementation should explicitly define how duplicate writes and concurrent executions are handled.

## 28.2 Business-Key Uniqueness

Deduplicating on `vehicle_id` and `event_time` within an incoming batch does not automatically remove duplicates that already exist in the target table under a different `db_id`.

The business key should be enforced consistently if it represents the intended definition of a unique observation.

## 28.3 Movement Speed Filtering

The movement-processing logic sets calculated speeds greater than 180 km/h to zero.

The GPS anomaly rule then checks for calculated speeds greater than 100 km/h and distances greater than 0.5 km.

Because values above 180 km/h are reset before the downstream rule runs, the anomaly detector may not see their original speed values.

If the intention is to preserve these events for anomaly detection, the original calculated speed should be retained separately from the filtered analytical speed.

## 28.4 Episode-Level Evaluation

The evaluation results depend on the implemented matching criteria.

Strict episode-level evaluation would provide stronger evidence by matching detections to ground-truth episodes using vehicle identity, anomaly type, and temporal overlap.

## 28.5 Streaming Retry Behavior

The pipeline writes rejected and valid records to separate Delta tables inside a `foreachBatch` function.

If a failure occurs between these writes, a retry can repeat one write while the other has not completed.

Production implementations should consider idempotent writes, batch identifiers, transactional design, and recovery testing.

## 28.6 Schema Evolution

The use of `mergeSchema` can allow compatible schema changes during writes.

However, schema evolution should be managed deliberately so that unexpected source changes do not silently create inconsistent downstream datasets.

## 28.7 Monitoring and Observability

Additional monitoring can track:

- Number of incoming records.
- Number of valid records.
- Number of rejected records.
- Duplicate counts.
- Processing duration.
- Number of detected anomalies.
- Number of aggregated episodes.
- Evaluation metric changes over time.

These measurements can help identify data quality problems and unexpected changes in pipeline behavior.

---

# 29. Final Summary

The Silver-layer workflow transforms raw fleet telemetry into curated and analytics-ready data.

The implementation includes:

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

The recorded evaluation results are:

| Metric | Result |
|---|---:|
| Precision | 1.0000 |
| Recall | 0.8333 |
| F1-score | 0.9091 |
| True positives | 5 |
| Total detected in scope | 5 |
| Total ground-truth episodes | 6 |

These results should be understood within the scope and limitations of the current evaluation logic.

Overall, the project demonstrates an end-to-end Silver processing workflow that combines data quality, incremental ingestion, telemetry enrichment, movement analytics, anomaly detection, episode aggregation, and analytical validation.

---
