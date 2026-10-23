# Fabric notebook: load the reference CSVs and the sample truth table into Delta tables.
# Paste into a Fabric notebook attached to the team Lakehouse, after uploading
# generator/output/Files/reference/*.csv to Files/reference and output/Files/sample/* to Files/sample.

from pyspark.sql import functions as F

REFERENCE = ["depots", "corridors", "corridor_points", "drivers_v1", "drivers_v2", "vehicles", "trips"]

for name in REFERENCE:
    df = (spark.read.option("header", True).option("inferSchema", True)
          .csv(f"Files/reference/{name}.csv"))
    df.write.format("delta").mode("overwrite").saveAsTable(f"ref_{name}")
    print(f"ref_{name}: {df.count()} rows")

# Truth table: used ONLY for the final evaluation. Detection code must never read it.
truth = spark.read.option("header", True).csv("Files/sample/telemetry_truth.csv")
truth.write.format("delta").mode("overwrite").saveAsTable("telemetry_truth")

# Quick checks that go into the evidence folder.
spark.sql("SELECT COUNT(DISTINCT vehicle_id) AS vehicles, "
          "SUM(CASE WHEN vehicle_type = 'refrigerated_truck' THEN 1 ELSE 0 END) AS reefers "
          "FROM ref_vehicles").show()
spark.sql("SELECT anomaly_type, COUNT(DISTINCT anomaly_group_id) AS episodes, COUNT(*) AS readings "
          "FROM telemetry_truth GROUP BY anomaly_type").show()
