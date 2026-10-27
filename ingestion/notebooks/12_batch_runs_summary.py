# Fabric notebook 12: evidence for the five batch runs of the v2 batch path (source DB -> extracts -> bronze).
#   Run 1 initial load (2026-11-02, all hours) | Run 2 incremental #1 (new hour)
#   Run 3 incremental #2 + re-run of an hour (idempotency) | Run 4 crash + retry | Run 5 backfill of an old hour

from pyspark.sql import functions as F

print("Every load of extract files, in order (read = rows in the new files, inserted = new rows in bronze,"
      " skipped = rows already in bronze, i.e. re-runs and backfills):")
(spark.table("bronze_ingestion_log").filter(F.col("stage") == "extract_load")
   .select("loaded_at_utc", F.col("events_received").alias("read"), F.col("events_written").alias("inserted"),
           F.col("events_rejected").alias("skipped_already_in_bronze"), "duration_s")
   .orderBy("loaded_at_utc").show(truncate=False))

print("Extract files per hour slice (more than one file = the hour was extracted again):")
(spark.table("landing_v2_files_loaded")
   .groupBy("slice_hour").agg(F.count("*").alias("files"), F.collect_list("extract_id").alias("extracts"),
                              F.max("rows_in_file").alias("rows"))
   .orderBy("slice_hour").show(40, truncate=False))

print("bronze_telemetry_v2: db_extract rows must equal distinct db_id (no duplicates after re-runs and backfills):")
(spark.table("bronze_telemetry_v2").filter(F.col("source") == "db_extract")
   .agg(F.count("*").alias("rows"), F.countDistinct("db_id").alias("distinct_db_ids"),
        F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("rejected_at_api")).show())
