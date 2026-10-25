# Fabric notebook 08: batch path v2 - load the database extracts into bronze_telemetry_v2.
#
# The extractor (platform/extractor/extract.py) writes one gzip JSON-lines file per hour slice of the
# source table telemetry_history to Files/landing_v2/event_date=.../event_hour=.../part-<extract_id>.jsonl.gz.
# This notebook:
#   1. finds files not loaded yet (half-uploaded files end in .uploading and are skipped),
#   2. inserts their rows into bronze_telemetry_v2 with source = 'db_extract' - INSERT ONLY, keyed on db_id:
#      a row that is already there is never inserted again, so re-runs and backfills add no duplicates,
#   3. records every file in landing_v2_files_loaded and moves it to Files/archive/landing_v2/,
#   4. writes one 'extract_load' row to bronze_ingestion_log.

import time
import uuid

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T

LANDING, ARCHIVE, TARGET = "Files/landing_v2", "Files/archive/landing_v2", "bronze_telemetry_v2"
try:
    fsu = notebookutils.fs  # noqa: F821 (built into Fabric notebooks)
except NameError:
    from notebookutils import mssparkutils
    fsu = mssparkutils.fs

spark.sql("""CREATE TABLE IF NOT EXISTS landing_v2_files_loaded (
    file_name STRING, extract_id STRING, slice_hour STRING, rows_in_file BIGINT, loaded_at_utc TIMESTAMP) USING DELTA""")
spark.sql("""CREATE TABLE IF NOT EXISTS bronze_ingestion_log (
    log_id STRING, stage STRING, run_id STRING, source STRING, interval_start_utc TIMESTAMP,
    duration_s DOUBLE, events_received BIGINT, events_written BIGINT, events_rejected BIGINT,
    retries BIGINT, active_vehicles BIGINT, events_per_s DOUBLE, loaded_at_utc TIMESTAMP) USING DELTA""")

EXTRACT_SCHEMA = T.StructType([
    T.StructField("db_id", T.LongType()), T.StructField("raw", T.StringType()),
    T.StructField("is_valid", T.BooleanType()), T.StructField("reject_reason", T.StringType()),
    T.StructField("run_id", T.StringType()), T.StructField("db_received_at", T.StringType()),
    T.StructField("slice_hour", T.StringType()), T.StructField("source", T.StringType()),
    T.StructField("extract_id", T.StringType())])


def list_files(path):
    try:
        entries = fsu.ls(path)
    except Exception:  # folder does not exist yet
        return []
    out = []
    for e in entries:
        out += list_files(f"{path}/{e.name}") if e.isDir else [(f"{path}/{e.name}", e.name)]
    return out


started = time.time()
loaded = {r.file_name for r in spark.table("landing_v2_files_loaded").select("file_name").collect()}
new_files = [(p, n) for p, n in list_files(LANDING) if n.endswith(".jsonl.gz") and n not in loaded]
print(f"1. {len(new_files)} new extract files")

if new_files:
    rows = (spark.read.schema(EXTRACT_SCHEMA).json([p for p, _ in new_files])
            .withColumn("file_name", F.col("_metadata.file_name")).cache())
    n_read = rows.count()
    target = spark.table(TARGET)
    before = target.filter(F.col("source") == "db_extract").count()

    # Same columns as the stream path writes. sent_at_utc = when the extractor exported the slice
    # (encoded in extract_id, e.g. x20261026T012345Z-1a2b3c); arrival in bronze = load time.
    batch = (rows.dropDuplicates(["db_id"])  # a re-run and the original may arrive in the same load
             .withColumn("sent_at_utc", F.date_format(F.to_timestamp(F.substring("extract_id", 2, 15),
                                                                     "yyyyMMdd'T'HHmmss"),
                                                      "yyyy-MM-dd'T'HH:mm:ss'Z'"))
             .withColumn("kafka_partition", F.lit(None).cast("long"))
             .withColumn("kafka_offset", F.lit(None).cast("long"))
             .withColumn("PartitionId", F.lit(None).cast("long"))
             .withColumn("EventEnqueuedUtcTime", F.current_timestamp())
             .withColumn("EventProcessedUtcTime", F.current_timestamp())
             .select(*target.columns))
    (DeltaTable.forName(spark, TARGET).alias("t")
     .merge(batch.alias("s"), "t.db_id = s.db_id AND t.source = 'db_extract'")
     .whenNotMatchedInsertAll().execute())
    inserted = spark.table(TARGET).filter(F.col("source") == "db_extract").count() - before
    print(f"2. read {n_read:,} rows, inserted {inserted:,} new, skipped {n_read - inserted:,} already in bronze")

    per_file = (rows.groupBy("file_name", "extract_id", "slice_hour").count()
                .select("file_name", "extract_id", "slice_hour", F.col("count").alias("rows_in_file"),
                        F.current_timestamp().alias("loaded_at_utc")))
    (DeltaTable.forName(spark, "landing_v2_files_loaded").alias("t")
     .merge(per_file.alias("s"), "t.file_name = s.file_name").whenNotMatchedInsertAll().execute())
    for p, _ in new_files:
        fsu.mv(p, p.replace(LANDING, ARCHIVE, 1), True)
    print(f"3. recorded and archived {len(new_files)} files")

    duration = time.time() - started
    log_id = f"extract-load-{uuid.uuid4()}"
    spark.createDataFrame([(log_id, "extract_load", log_id, "db_extract", round(duration, 2), n_read, inserted,
                            n_read - inserted, round(inserted / duration, 1))],
                          "log_id string, stage string, run_id string, source string, duration_s double, "
                          "events_received long, events_written long, events_rejected long, events_per_s double") \
        .select("log_id", "stage", "run_id", "source", F.current_timestamp().alias("interval_start_utc"),
                "duration_s", "events_received", "events_written",
                "events_rejected",  # here: rows skipped because they were already in bronze
                F.lit(None).cast("long").alias("retries"), F.lit(None).cast("long").alias("active_vehicles"),
                "events_per_s", F.current_timestamp().alias("loaded_at_utc")) \
        .write.format("delta").mode("append").saveAsTable("bronze_ingestion_log")
    print(f"4. logged in {duration:.1f} s")

# Evidence
print("Extract files loaded, per hour slice (a re-run or backfill shows up as a second file for the same hour):")
spark.sql("""SELECT slice_hour, count(*) AS files, collect_list(extract_id) AS extracts, max(rows_in_file) AS rows
             FROM landing_v2_files_loaded GROUP BY slice_hour ORDER BY slice_hour""").show(60, truncate=False)
print("bronze_telemetry_v2 by source (rows must equal distinct db_id for db_extract):")
(spark.table(TARGET).groupBy("source")
   .agg(F.count("*").alias("rows"), F.countDistinct("db_id").alias("distinct_db_ids"),
        F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("rejected_at_api"))
   .orderBy("source").show())
