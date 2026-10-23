# Fabric notebook: check a full fleet run in bronze_telemetry.
# Set RUN_ID to the run_id printed by `python run_fleet.py` (also in output/runs/<run_id>/perf.json).

from pyspark.sql import functions as F

RUN_ID = "full-20261023T141747Z"  # the first 5,000-vehicle run

run = spark.table("bronze_telemetry").filter(F.col("run_id") == RUN_ID)

# 1) Rows received for this run (compare with events_sent in perf.json).
print("rows in bronze:", run.count())

# 2) The "done when" check: 5,000 different vehicle IDs in bronze.
(run.select(F.get_json_object("raw", "$.vehicle_id").alias("vehicle_id"))
    .agg(F.countDistinct("vehicle_id").alias("distinct_vehicles")).show())

# 3) Arrival window and ingestion lag (Eventstream arrival time vs sender time).
(run.select(F.min("EventEnqueuedUtcTime").alias("first_arrival"),
            F.max("EventEnqueuedUtcTime").alias("last_arrival"),
            F.expr("avg(unix_timestamp(EventEnqueuedUtcTime) - unix_timestamp(to_timestamp(sent_at_utc)))")
             .alias("avg_lag_s"))
    .show(truncate=False))

# 4) Rows per Eventstream partition (each vehicle always uses the same partition).
run.groupBy("PartitionId").count().orderBy("PartitionId").show()
