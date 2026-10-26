# Fabric notebook 10: check what reached the cloud through the new path
#   generator -> API -> PostgreSQL -> Debezium -> Kafka -> bridge -> fleet_eventstream_v2 -> bronze_telemetry_v2
# Compare the numbers with the source database (API /v1/stats) and the bridge log.

from pyspark.sql import functions as F

v2 = spark.table("bronze_telemetry_v2")
v2.printSchema()

# 1) Rows per generator run, and how many distinct source rows (db_id) they cover.
#    rows > distinct_db_ids would mean the bridge re-sent something after a restart (at-least-once);
#    db_id is the key that removes those duplicates downstream.
(v2.groupBy("run_id", "source")
   .agg(F.count("*").alias("rows"), F.countDistinct("db_id").alias("distinct_db_ids"),
        F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("rejected_at_api"),
        F.min("EventEnqueuedUtcTime").alias("first_arrival"), F.max("EventEnqueuedUtcTime").alias("last_arrival"))
   .orderBy("first_arrival").show(truncate=False))

# 2) Rejected rows by reason type (same rules as the API and notebook 04).
(v2.filter(~F.col("is_valid"))
   .groupBy(F.split("reject_reason", ":")[0].alias("reason_type")).count().orderBy("reason_type").show())

# 3) End-to-end latency per streaming run: from the moment the database stored the row to arrival in Fabric.
(v2.filter(F.col("source") == "kafka_cdc")
   .withColumn("db_to_cloud_s", F.col("EventEnqueuedUtcTime").cast("double")
               - F.to_timestamp("db_received_at").cast("double"))
   .groupBy("run_id")
   .agg(F.round(F.avg("db_to_cloud_s"), 2).alias("avg_s"),
        F.round(F.expr("percentile_approx(db_to_cloud_s, 0.5)"), 2).alias("p50_s"),
        F.round(F.expr("percentile_approx(db_to_cloud_s, 0.95)"), 2).alias("p95_s"),
        F.round(F.max("db_to_cloud_s"), 2).alias("max_s"))
   .orderBy("run_id").show(truncate=False))

# 4) Cloud arrivals per minute for the streaming runs (steady rate = a flat line around 700 x 60 = 42,000).
(v2.filter(F.col("source") == "kafka_cdc")
   .groupBy("run_id", F.date_trunc("minute", "EventEnqueuedUtcTime").alias("minute")).count()
   .groupBy("run_id").agg(F.count("*").alias("minutes"), F.min("count").alias("min_per_min"),
                          F.expr("percentile_approx(count, 0.5)").alias("median_per_min"),
                          F.max("count").alias("max_per_min"))
   .orderBy("run_id").show(truncate=False))
