#!/usr/bin/env python
# coding: utf-8

# ## 08_anomaly_detection_and_evaluation
# 
# null

# In[1]:


import time
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, BooleanType, LongType
)

notebook_start_time = time.time()

df_silver=spark.read.table("silver_telemetry")

try:
    df_ref_vehicle=spark.read.table("ref_vehicles") 
    df_enriched=df_silver.join(F.broadcast(df_ref_vehicle), on="vehicle_id", how="left")
    print("Successfully joined telemetry with reference data via Broadcast Join.")
except Exception as e:
    print(f"Proceeding directly with silver_telemetry ({e})")
    df_enriched = df_silver

display(df_enriched.limit(5))
# just here made a broadcast join with vehicles ref table and silver table to send that small table to all worker nodes to avoid shuffling 


# In[2]:


window_spec=Window.partitionBy("vehicle_id").orderBy("event_time")

df_windowed= df_enriched  \
   .withColumn("prev_latitude",F.lag("latitude").over(window_spec)) \
   .withColumn("prev_longitude",F.lag("longitude").over(window_spec)) \
   .withColumn("prev_event_time", F.lag("event_time").over(window_spec))

df_windowed= df_windowed.withColumn("time_gap_sec",F.unix_timestamp("event_time") - F.unix_timestamp("prev_event_time"))

haversine_expr = F.expr("""
    2 * 6371 * asin(
        sqrt(
            power(sin(radians((latitude - prev_latitude) / 2)), 2) +
            cos(radians(prev_latitude)) * cos(radians(latitude)) *
            power(sin(radians((longitude - prev_longitude) / 2)), 2)
        )
    )
""") 
# just search for haversine formula it is pure math to calculate the distance in kilometer

df_helpers=df_windowed.withColumn("distance_km",F.when(F.col("prev_latitude").isNotNull(), haversine_expr).otherwise(0.0)) 

df_helpers = df_helpers.withColumn(
    "calculated_speed_kmh",
    F.when((F.col("time_gap_sec").isNotNull()) & (F.col("time_gap_sec") > 0),
           (F.col("distance_km") / (F.col("time_gap_sec") / 3600.0)))
    .otherwise(0.0)
) 

df_helpers = df_helpers \
    .withColumn("is_idling", F.when((F.col("speed_kmh") < 2) & (F.col("rpm") > 0), True).otherwise(False)) \
    .withColumn("is_cold_chain_violation", 
                F.when(F.col("cargo_temp_c").isNotNull() & ((F.col("cargo_temp_c") < 2) | (F.col("cargo_temp_c") > 8)), True)
                .otherwise(False))


display(df_helpers.limit(100))





# Calculate vehicle movement metrics using window functions, including previous GPS coordinates, timestamps, time gaps, and Haversine distance.
# Estimate vehicle speed in km/h and identify idling vehicles based on speed and engine RPM.
# Detect cold chain violations when cargo temperature falls outside the allowed range of 2°C to 8°C.


    


# In[3]:


spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.advise.divisionExprConvertRule.enable", "true")


df_anomalies = df_helpers \
    .withColumn("is_gps_teleportation", F.when(F.col("calculated_speed_kmh") > 200, True).otherwise(False)) \
    .withColumn("is_engine_overheating", F.when(F.col("engine_temp_c") > 105, True).otherwise(False)) \
    .withColumn("is_rpm_spike", F.when(F.col("rpm") > 4500, True).otherwise(False)) \
    .withColumn("is_cold_chain_breach", F.col("is_cold_chain_violation")) \
    .withColumn("is_excessive_idling", F.col("is_idling"))

df_anomalies = df_anomalies.withColumn(
    "is_anomaly",
    F.col("is_gps_teleportation") | 
    F.col("is_engine_overheating") | 
    F.col("is_rpm_spike") | 
    F.col("is_cold_chain_breach") | 
    F.col("is_excessive_idling")
)

df_anomalies = df_anomalies.withColumn(
    "anomaly_type",
    F.when(F.col("is_cold_chain_breach"), "cold_chain_breach")
     .when(F.col("is_excessive_idling"), "excessive_idling")
     .when(F.col("is_gps_teleportation"), "gps_teleportation")
     .when(F.col("is_engine_overheating"), "engine_overheating")
     .when(F.col("is_rpm_spike"), "rpm_spike")
     .otherwise(None)
)

df_anomalies.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_telemetry_anomalies")

episodes_window = Window.partitionBy("vehicle_id", "anomaly_type").orderBy("event_time")

df_flagged = df_anomalies.filter(F.col("is_anomaly") == True) \
    .withColumn("prev_anomaly_time", F.lag("event_time").over(episodes_window)) \
    .withColumn("gap_minutes", (F.unix_timestamp("event_time") - F.unix_timestamp("prev_anomaly_time")) / 60.0)

df_flagged = df_flagged.withColumn(
    "new_episode_flag", 
    F.when((F.col("gap_minutes").isNull()) | (F.col("gap_minutes") > 5), 1).otherwise(0)
)

group_window = Window.partitionBy("vehicle_id", "anomaly_type").orderBy("event_time").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_flagged = df_flagged.withColumn("episode_id", F.sum("new_episode_flag").over(group_window))

df_episodes = df_flagged.groupBy("vehicle_id", "anomaly_type", "episode_id").agg(
    F.min("event_time").alias("start_time"),
    F.max("event_time").alias("end_time"),
    F.count("*").alias("reading_count"),
    F.max("calculated_speed_kmh").alias("max_calculated_speed")
).withColumn("duration_seconds", F.unix_timestamp("end_time") - F.unix_timestamp("start_time"))

df_episodes.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_anomaly_episodes")


# In[4]:


try:
    df_truth = spark.read.table("telemetry_truth")
    df_detected_episodes = spark.read.table("silver_anomaly_episodes")

    df_truth_extracted = df_truth.withColumn(
        "extracted_vehicle_id",
        F.regexp_extract(F.col("anomaly_group_id"), r"(VEH-\d+)", 1)
    )

    tp_df = df_detected_episodes.join(
        df_truth_extracted,
        (df_detected_episodes["vehicle_id"] == df_truth_extracted["extracted_vehicle_id"]) &
        (F.lower(df_detected_episodes["anomaly_type"]) == F.lower(df_truth_extracted["anomaly_type"])),
        "inner"
    ).select(df_detected_episodes["vehicle_id"], df_detected_episodes["episode_id"]).distinct()

    tp_count = tp_df.count()
    detected_count = df_detected_episodes.count()
    truth_episodes_count = df_truth_extracted.select("anomaly_group_id").distinct().count()

    precision = (tp_count / detected_count) if detected_count > 0 else 0.0
    recall = (tp_count / truth_episodes_count) if truth_episodes_count > 0 else 0.0
    f1_score = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0

    print("==========================================")
    print("📊 EVALUATION METRICS REPORT")
    print("==========================================")
    print(f"True Positive Episodes (TP) : {tp_count}")
    print(f"Total Detected Episodes     : {detected_count}")
    print(f"Total Ground Truth Groups   : {truth_episodes_count}")
    print(f"Precision                   : {precision:.4f} ({precision * 100:.2f}%)")
    print(f"Recall                      : {recall:.4f} ({recall * 100:.2f}%)")
    print(f"F1 Score                    : {f1_score:.4f}")
    print("==========================================")

    eval_schema = StructType([
        StructField("precision", DoubleType(), True),
        StructField("recall", DoubleType(), True),
        StructField("f1_score", DoubleType(), True),
        StructField("true_positives", LongType(), True),
        StructField("total_detected", LongType(), True),
        StructField("total_truth", LongType(), True)
    ])
    
    eval_data = [(float(precision), float(recall), float(f1_score), int(tp_count), int(detected_count), int(truth_episodes_count))]
    
    df_eval = spark.createDataFrame(eval_data, eval_schema)
    df_eval.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("evaluation_metrics")
    
    display(df_eval)

except Exception as e:
    print(f" Exception during evaluation: {e}")

