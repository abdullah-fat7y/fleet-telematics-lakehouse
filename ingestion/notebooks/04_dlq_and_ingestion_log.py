# Fabric notebook 04: dead-letter queue (dlq_telemetry) and ingestion log (bronze_ingestion_log).
#
# Before running, upload from generator/output on the sender machine to the Lakehouse folder Files/ops:
#   dlq/send_failures.jsonl, logs/send_log.csv, logs/ingestion_intervals.csv   (missing files are skipped)
#
# What it does (safe to re-run; every write is a MERGE on a stable id):
#   A. Validates bronze rows that arrived since the last run. Rows that cannot be read or fail
#      validation go to dlq_telemetry (stage = 'validation'). Bronze itself is never changed.
#   B. Loads events the sender gave up on (stage = 'send') into dlq_telemetry.
#   C. Marks 'send' DLQ rows as recovered once the same raw event is in bronze with source = 'dlq_replay'.
#   D. Loads the sender's run and 10-second interval logs, plus this validation pass, into bronze_ingestion_log.

import time
import uuid

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.conf.set("spark.sql.ansi.enabled", "false")  # bad casts give null instead of failing the job

OPS = "Files/ops"
SETTLE_MINUTES = 2  # only validate rows older than this, so late-landing micro-batches are not skipped
FIELDS = ["event_id", "vehicle_id", "trip_id", "event_time", "latitude", "longitude", "speed_kmh",
          "rpm", "engine_temp_c", "fuel_rate_lph", "odometer_km", "cargo_temp_c"]
REQUIRED = [f for f in FIELDS if f != "cargo_temp_c"]  # cargo_temp_c is empty for dry vans by design
NUMERIC = ["latitude", "longitude", "speed_kmh", "rpm", "engine_temp_c", "fuel_rate_lph", "odometer_km", "cargo_temp_c"]

spark.sql("""CREATE TABLE IF NOT EXISTS dlq_telemetry (
    dlq_id STRING, stage STRING, reason STRING, raw STRING, source STRING, run_id STRING,
    arrived_at_utc TIMESTAMP, failed_at_utc TIMESTAMP, retries INT, status STRING,
    recovered_at_utc TIMESTAMP, recovered_run_id STRING, loaded_at_utc TIMESTAMP) USING DELTA""")
spark.sql("""CREATE TABLE IF NOT EXISTS bronze_ingestion_log (
    log_id STRING, stage STRING, run_id STRING, source STRING, interval_start_utc TIMESTAMP,
    duration_s DOUBLE, events_received BIGINT, events_written BIGINT, events_rejected BIGINT,
    retries BIGINT, active_vehicles BIGINT, events_per_s DOUBLE, loaded_at_utc TIMESTAMP) USING DELTA""")
spark.sql("""CREATE TABLE IF NOT EXISTS ingestion_watermark (
    job STRING, last_enqueued_utc TIMESTAMP, updated_at_utc TIMESTAMP) USING DELTA""")


def insert_new(table, df, key):
    """Insert rows whose key is not in the table yet (makes every load safe to repeat)."""
    (DeltaTable.forName(spark, table).alias("t")
     .merge(df.alias("s"), f"t.{key} = s.{key}")
     .whenNotMatchedInsertAll().execute())


def read_ops(reader):
    try:
        return reader()
    except Exception as e:  # file not uploaded yet
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e):
            return None
        raise


# ---------- A. Validate new bronze rows ----------
started = time.time()
wm = spark.sql("SELECT max(last_enqueued_utc) FROM ingestion_watermark WHERE job = 'bronze_validation'").first()[0]
bronze = spark.table("bronze_telemetry")
new = bronze.filter(F.col("EventEnqueuedUtcTime") <= F.expr(f"current_timestamp() - INTERVAL {SETTLE_MINUTES} MINUTES"))
if wm is not None:
    new = new.filter(F.col("EventEnqueuedUtcTime") > F.lit(wm))
new = new.cache()

as_text = T.StructType([T.StructField(f, T.StringType()) for f in FIELDS])
checked = (new
           .withColumn("is_json", F.get_json_object("raw", "$").isNotNull())
           .withColumn("keys", F.when(F.col("is_json"), F.json_object_keys("raw")))
           .withColumn("p", F.from_json("raw", as_text)))
checks = [F.when(~F.col("is_json"), F.lit("malformed_json: not valid JSON"))]
for f in REQUIRED:
    checks.append(F.when(F.col("is_json") & ~F.array_contains("keys", f), F.lit(f"missing_field: {f}")))
    checks.append(F.when(F.col("is_json") & F.array_contains("keys", f) & F.col(f"p.{f}").isNull(),
                         F.lit(f"null_required: {f}")))
for f in NUMERIC:
    checks.append(F.when(F.col(f"p.{f}").isNotNull() & F.col(f"p.{f}").cast("double").isNull(),
                         F.lit(f"wrong_type: {f} is not a number")))
checks.append(F.when(F.col("p.event_time").isNotNull()
                     & F.to_timestamp("p.event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'").isNull(),
                     F.lit("wrong_type: event_time is not ISO 8601 UTC")))
checked = checked.withColumn("reasons", F.filter(F.array(*checks), lambda r: r.isNotNull())).cache()

rejected = (checked.filter(F.size("reasons") > 0)
            .select(F.sha2(F.concat_ws("|", "run_id", F.col("PartitionId").cast("string"),
                                       F.col("EventEnqueuedUtcTime").cast("string"), "raw"), 256).alias("dlq_id"),
                    F.lit("validation").alias("stage"),
                    F.array_join("reasons", "; ").alias("reason"),
                    "raw", "source", "run_id",
                    F.col("EventEnqueuedUtcTime").alias("arrived_at_utc"),
                    F.current_timestamp().alias("failed_at_utc"),
                    F.lit(0).alias("retries"),
                    F.lit("open").alias("status"),
                    F.lit(None).cast("timestamp").alias("recovered_at_utc"),
                    F.lit(None).cast("string").alias("recovered_run_id"),
                    F.current_timestamp().alias("loaded_at_utc")))
insert_new("dlq_telemetry", rejected, "dlq_id")

n_received = checked.count()
n_rejected = checked.filter(F.size("reasons") > 0).count()
n_vehicles = checked.select(F.get_json_object("raw", "$.vehicle_id")).distinct().count()
latest = checked.agg(F.max("EventEnqueuedUtcTime")).first()[0]
print(f"A. validated {n_received} new bronze rows: {n_received - n_rejected} valid, {n_rejected} to DLQ")

# ---------- B. Events the sender gave up on ----------
send_schema = T.StructType([T.StructField(c, T.StringType()) for c in
                            ["raw", "source", "reason", "failed_at_utc", "run_id"]] +
                           [T.StructField("retries", T.IntegerType())])
send_dlq = read_ops(lambda: spark.read.schema(send_schema).json(f"{OPS}/send_failures.jsonl"))
if send_dlq is not None:
    insert_new("dlq_telemetry", send_dlq.select(
        F.sha2(F.concat_ws("|", "run_id", "failed_at_utc", "raw"), 256).alias("dlq_id"),
        F.lit("send").alias("stage"), "reason", "raw", "source", "run_id",
        F.lit(None).cast("timestamp").alias("arrived_at_utc"),
        F.to_timestamp("failed_at_utc").alias("failed_at_utc"), "retries",
        F.lit("open").alias("status"),
        F.lit(None).cast("timestamp").alias("recovered_at_utc"),
        F.lit(None).cast("string").alias("recovered_run_id"),
        F.current_timestamp().alias("loaded_at_utc")), "dlq_id")
    print(f"B. read {send_dlq.count()} send-failure entries (only new ones are inserted)")
else:
    print("B. no Files/ops/send_failures.jsonl - skipped")

# ---------- C. Recovery: DLQ rows whose replayed copy is now in bronze ----------
replayed = (bronze.filter(F.col("source") == "dlq_replay")
            .groupBy("raw").agg(F.min("EventEnqueuedUtcTime").alias("recovered_at_utc"),
                                F.first("run_id").alias("recovered_run_id")))
(DeltaTable.forName(spark, "dlq_telemetry").alias("d")
 .merge(replayed.alias("r"), "d.raw = r.raw AND d.stage = 'send' AND d.status = 'open'")
 .whenMatchedUpdate(set={"status": F.lit("recovered"), "recovered_at_utc": "r.recovered_at_utc",
                         "recovered_run_id": "r.recovered_run_id"})
 .execute())

# ---------- D. Ingestion log ----------
intervals = read_ops(lambda: spark.read.option("header", True).csv(f"{OPS}/ingestion_intervals.csv"))
if intervals is not None:
    insert_new("bronze_ingestion_log", intervals.select(
        F.sha2(F.concat_ws("|", F.lit("sender_interval"), "run_id", "interval_start_utc"), 256).alias("log_id"),
        F.lit("sender_interval").alias("stage"), "run_id", F.lit("stream").alias("source"),
        F.to_timestamp("interval_start_utc").alias("interval_start_utc"),
        F.col("duration_s").cast("double").alias("duration_s"),
        F.col("events_received").cast("long").alias("events_received"),
        F.col("events_written").cast("long").alias("events_written"),
        F.col("events_rejected").cast("long").alias("events_rejected"),
        F.lit(None).cast("long").alias("retries"),
        F.col("active_vehicles").cast("long").alias("active_vehicles"),
        F.col("events_per_s").cast("double").alias("events_per_s"),
        F.current_timestamp().alias("loaded_at_utc")), "log_id")

runs = read_ops(lambda: spark.read.option("header", True).csv(f"{OPS}/send_log.csv"))
if runs is not None:
    insert_new("bronze_ingestion_log", runs.filter(F.lower("dry_run") == "false").select(
        F.sha2(F.concat_ws("|", F.lit("sender_run"), "run_id"), 256).alias("log_id"),
        F.lit("sender_run").alias("stage"), "run_id", "source",
        F.to_timestamp("started_utc").alias("interval_start_utc"),
        F.col("duration_s").cast("double").alias("duration_s"),
        F.col("events_read").cast("long").alias("events_received"),
        F.col("events_sent").cast("long").alias("events_written"),
        F.col("events_dlq").cast("long").alias("events_rejected"),
        F.col("retries").cast("long").alias("retries"),
        F.lit(None).cast("long").alias("active_vehicles"),
        F.col("events_per_s").cast("double").alias("events_per_s"),
        F.current_timestamp().alias("loaded_at_utc")), "log_id")

duration = time.time() - started
batch_id = f"validation-{uuid.uuid4()}"
insert_new("bronze_ingestion_log", spark.createDataFrame([(
    batch_id, "bronze_validation", batch_id, "bronze", None, round(duration, 2), n_received,
    n_received - n_rejected, n_rejected, None, n_vehicles,
    round(n_received / duration, 1) if duration else None)],
    "log_id string, stage string, run_id string, source string, interval_start_utc timestamp, duration_s double, "
    "events_received long, events_written long, events_rejected long, retries long, active_vehicles long, "
    "events_per_s double").withColumn("interval_start_utc", F.current_timestamp())
    .withColumn("loaded_at_utc", F.current_timestamp()), "log_id")

if latest is not None:  # move the watermark only after everything above succeeded
    spark.createDataFrame([("bronze_validation", latest)], "job string, last_enqueued_utc timestamp") \
        .withColumn("updated_at_utc", F.current_timestamp()).createOrReplaceTempView("wm_new")
    spark.sql("""MERGE INTO ingestion_watermark t USING wm_new s ON t.job = s.job
                 WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")

# ---------- Evidence ----------
print("DLQ by stage and reason type:")
spark.sql("""SELECT stage, split(reason, ':')[0] AS reason_type, status, count(*) AS rows
             FROM dlq_telemetry GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""").show(truncate=False)
print("Example DLQ rows:")
spark.table("dlq_telemetry").select("stage", "reason", F.substring("raw", 1, 80).alias("raw_start"),
                                    "retries", "status").orderBy(F.rand(7)).show(6, truncate=False)
print("Ingestion log (latest first):")
spark.table("bronze_ingestion_log").orderBy(F.desc("loaded_at_utc"), F.desc("interval_start_utc")) \
    .select("stage", "run_id", "interval_start_utc", "events_received", "events_written", "events_rejected",
            "retries", "events_per_s").show(12, truncate=False)
