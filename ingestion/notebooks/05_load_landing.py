# Fabric notebook 05: batch path - load landing files into bronze.
#
# Batch files (one raw event per line, written by `run_fleet.py --mode batch` and uploaded by
# `upload_to_onelake.py landing`) sit in Files/landing/<run_id>/. This notebook:
#   1. finds .jsonl files not loaded yet (half-uploaded files end in .uploading and are skipped),
#   2. appends every line to bronze_telemetry exactly as received, with source = 'batch' -
#      same columns as the stream path, bronze is only ever appended to,
#   3. records each file in landing_files_loaded and moves it to Files/archive/landing/,
#   4. writes one 'batch_load' row to bronze_ingestion_log.
# Validation and the DLQ for batch rows happen in notebook 04, exactly as for streamed rows.

import time
import uuid

from delta.tables import DeltaTable
from pyspark.sql import functions as F

LANDING, ARCHIVE = "Files/landing", "Files/archive/landing"
try:
    fsu = notebookutils.fs  # noqa: F821 (built into Fabric notebooks)
except NameError:
    from notebookutils import mssparkutils
    fsu = mssparkutils.fs

spark.sql("""CREATE TABLE IF NOT EXISTS landing_files_loaded (
    file_name STRING, run_id STRING, rows BIGINT, file_written_utc STRING, loaded_at_utc TIMESTAMP) USING DELTA""")
spark.sql("""CREATE TABLE IF NOT EXISTS bronze_ingestion_log (
    log_id STRING, stage STRING, run_id STRING, source STRING, interval_start_utc TIMESTAMP,
    duration_s DOUBLE, events_received BIGINT, events_written BIGINT, events_rejected BIGINT,
    retries BIGINT, active_vehicles BIGINT, events_per_s DOUBLE, loaded_at_utc TIMESTAMP) USING DELTA""")


def list_files(path):
    try:
        entries = fsu.ls(path)
    except Exception:  # folder does not exist yet
        return []
    files = []
    for e in entries:
        files += list_files(f"{path}/{e.name}") if e.isDir else [(f"{path}/{e.name}", e.name)]
    return files


started = time.time()
loaded = {r.file_name for r in spark.table("landing_files_loaded").select("file_name").collect()}
new_files = [(p, n) for p, n in list_files(LANDING) if n.endswith(".jsonl") and n not in loaded]
print(f"1. {len(new_files)} new landing files")

if new_files:
    lines = (spark.read.text([p for p, _ in new_files])
             .select(F.col("value").alias("raw"), F.col("_metadata.file_name").alias("file_name"))
             .filter(F.col("raw") != "")
             # file name: <run_id>_w<worker>_<seq>_<written at, e.g. 20261023T145920975754Z>.jsonl
             .withColumn("run_id", F.regexp_extract("file_name", r"^(batch-\d{8}T\d{6}Z)_", 1))
             .withColumn("stamp", F.regexp_extract("file_name", r"_(\d{8}T\d{6})(\d{6})Z\.jsonl$", 1))
             .withColumn("micro", F.regexp_extract("file_name", r"_(\d{8}T\d{6})(\d{6})Z\.jsonl$", 2))
             .withColumn("sent_at_utc", F.concat(F.date_format(F.to_timestamp("stamp", "yyyyMMdd'T'HHmmss"),
                                                               "yyyy-MM-dd'T'HH:mm:ss"),
                                                 F.lit("."), F.col("micro"), F.lit("Z")))
             .cache())

    # 2. Append to bronze with the same columns the Eventstream writes. Arrival time = load time.
    bronze_cols = spark.table("bronze_telemetry").columns
    to_bronze = (lines.withColumn("source", F.lit("batch"))
                 .withColumn("EventEnqueuedUtcTime", F.current_timestamp())
                 .withColumn("EventProcessedUtcTime", F.current_timestamp())
                 .withColumn("PartitionId", F.lit(None).cast("long"))
                 .select(*bronze_cols))
    to_bronze.write.format("delta").mode("append").saveAsTable("bronze_telemetry")
    n_rows = lines.count()
    print(f"2. appended {n_rows} rows to bronze_telemetry (source = 'batch')")

    # 3. Record the files, then move them out of landing.
    per_file = (lines.groupBy("file_name", "run_id", "sent_at_utc").count()
                .select("file_name", "run_id", F.col("count").alias("rows"),
                        F.col("sent_at_utc").alias("file_written_utc"), F.current_timestamp().alias("loaded_at_utc")))
    (DeltaTable.forName(spark, "landing_files_loaded").alias("t")
     .merge(per_file.alias("s"), "t.file_name = s.file_name").whenNotMatchedInsertAll().execute())
    for p, n in new_files:
        fsu.mv(p, p.replace(LANDING, ARCHIVE, 1), True)
    print(f"3. recorded and archived {len(new_files)} files")

    # 4. Ingestion log row for this load.
    duration = time.time() - started
    n_vehicles = lines.select(F.get_json_object("raw", "$.vehicle_id")).distinct().count()
    log_id = f"batch-load-{uuid.uuid4()}"
    spark.createDataFrame([(log_id, "batch_load", log_id, "batch", round(duration, 2), len(new_files), n_rows, 0,
                            n_vehicles, round(n_rows / duration, 1))],
                          "log_id string, stage string, run_id string, source string, duration_s double, "
                          "files long, events_written long, events_rejected long, active_vehicles long, "
                          "events_per_s double") \
        .select("log_id", "stage", "run_id", "source", F.current_timestamp().alias("interval_start_utc"),
                "duration_s", F.col("events_written").alias("events_received"), "events_written",
                "events_rejected", F.lit(None).cast("long").alias("retries"), "active_vehicles", "events_per_s",
                F.current_timestamp().alias("loaded_at_utc")) \
        .write.format("delta").mode("append").saveAsTable("bronze_ingestion_log")
    print(f"4. logged: {len(new_files)} files, {n_rows} rows, {n_vehicles} vehicles in {duration:.1f} s")

# Evidence
print("Bronze rows by source:")
spark.table("bronze_telemetry").groupBy("source").count().orderBy("source").show()
print("Landing files loaded per batch run:")
spark.sql("""SELECT run_id, count(*) AS files, sum(rows) AS rows, min(loaded_at_utc) AS loaded_at_utc
             FROM landing_files_loaded GROUP BY run_id ORDER BY run_id""").show(truncate=False)
