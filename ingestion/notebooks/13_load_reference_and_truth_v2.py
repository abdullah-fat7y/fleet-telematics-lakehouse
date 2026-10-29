# Fabric notebook 13: refresh reference trips and load the v2 truth tables.
#
# Run after `upload_to_onelake.py reference` and `upload_to_onelake.py truth` (push_new_data.sh does both):
#   - ref_trips          <- Files/reference/trips.csv (all simulated days; day 1 rows are unchanged)
#   - telemetry_truth_v2 <- Files/truth_v2/<run_id>.csv, one file per v2 generator run, with a run_id column
# The pilot table telemetry_truth (1,000-event sample) is left as it is.
# Truth tables are for evaluating detection only - detection code must never read them.

from pyspark.sql import functions as F

trips = spark.read.option("header", True).option("inferSchema", True).csv("Files/reference/trips.csv")
trips.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("ref_trips")
print("ref_trips:", trips.count(), "trips,", trips.select(F.to_date("planned_start_utc").alias("d")).distinct().count(),
      "days")

truth = (spark.read.option("header", True).csv("Files/truth_v2/*.csv")
         .withColumn("run_id", F.regexp_extract(F.col("_metadata.file_name"), r"^(.*)\.csv$", 1)))
truth.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("telemetry_truth_v2")

print("telemetry_truth_v2 per run:")
(spark.table("telemetry_truth_v2").groupBy("run_id", "anomaly_type")
   .agg(F.countDistinct("anomaly_group_id").alias("episodes"), F.count("*").alias("readings"))
   .orderBy("run_id", "anomaly_type").show(60, truncate=False))
