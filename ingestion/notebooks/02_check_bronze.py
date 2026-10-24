# Fabric notebook: check what the Eventstream wrote into bronze_telemetry.
# Run after `python send_stream.py`. Each cell's output is evidence.

from pyspark.sql import functions as F

bronze = spark.table("bronze_telemetry")
bronze.printSchema()  # expect raw, source, sent_at_utc, run_id (+ Eventstream system columns, if added)

# 1) How many raw lines arrived per send run.
bronze.groupBy("run_id", "source").count().show(truncate=False)

# 2) Distinct vehicles, read straight out of the raw text (malformed lines simply give null here).
(bronze.select(F.get_json_object("raw", "$.vehicle_id").alias("vehicle_id"))
       .agg(F.countDistinct("vehicle_id").alias("distinct_vehicles")).show())

# 3) Malformed lines are kept in bronze as text, not lost.
print("lines that are not valid JSON:",
      bronze.filter(F.get_json_object("raw", "$.event_id").isNull()
                    & F.get_json_object("raw", "$.vehicle_id").isNull()).count())

# 4) Append-only proof: the table history should only show write/append operations
#    (Fabric may also add OPTIMIZE compactions - those rewrite files, never change rows).
spark.sql("DESCRIBE HISTORY bronze_telemetry").select("version", "timestamp", "operation").show(truncate=False)
