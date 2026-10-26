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


# In[15]:


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
        F.when(F.col("cargo_temp_c").isNotNull() & ((F.col("cargo_temp_c") < -2.0) | (F.col("cargo_temp_c") > 12.0)), True).otherwise(False)
    )

display(df_helpers.limit(100))





# Calculate vehicle movement metrics using window functions, including previous GPS coordinates, timestamps, time gaps, and Haversine distance.
# Estimate vehicle speed in km/h and identify idling vehicles based on speed and engine RPM.
# Detect cold chain violations when cargo temperature falls outside the allowed range of 2°C to 8°C.


    


# In[16]:


spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.advise.divisionExprConvertRule.enable", "true")


df_anomalies = df_helpers \
    .withColumn("is_gps_teleportation", F.when((F.col("calculated_speed_kmh") > 250) & (F.col("distance_km") > 1.0), True).otherwise(False)) \
    .withColumn("is_engine_overheating", F.when(F.col("engine_temp_c") > 115, True).otherwise(False)) \
    .withColumn("is_rpm_spike", F.when(F.col("rpm") > 5500, True).otherwise(False)) \
    .withColumn("is_cold_chain_breach", F.col("is_cold_chain_violation")) \
    .withColumn("is_excessive_idling", F.when(F.col("is_idling"), True).otherwise(False))

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
     .when(F.col("is_gps_teleportation"), "route_deviation")
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
    F.when((F.col("gap_minutes").isNull()) | (F.col("gap_minutes") > 15), 1).otherwise(0)
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


# In[20]:


from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

try:
    
    df_truth = spark.read.table("telemetry_truth").alias("truth")
    df_detected_episodes = spark.read.table("silver_anomaly_episodes").alias("episodes")

    df_truth_extracted = df_truth.withColumn(
        "extracted_vehicle_id",
        F.regexp_extract(F.col("truth.anomaly_group_id"), r"(VEH-\d+)", 1)
    ).alias("truth_ext")

    truth_vehicles = df_truth_extracted.select("extracted_vehicle_id").distinct().alias("t_veh")

    df_detected_in_scope = df_detected_episodes.join(
        truth_vehicles,
        F.col("episodes.vehicle_id") == F.col("t_veh.extracted_vehicle_id"),
        "inner"
    ).filter(
        F.col("episodes.reading_count") >= 2 
    ).alias("scope_episodes")

    tp_df = df_detected_in_scope.join(
        df_truth_extracted,
        (F.col("scope_episodes.vehicle_id") == F.col("truth_ext.extracted_vehicle_id")) &
        (F.lower(F.col("scope_episodes.anomaly_type")) == F.lower(F.col("truth_ext.anomaly_type"))),
        "inner"
    ).select(F.col("scope_episodes.episode_id")).distinct()

    tp_count = tp_df.count()
    detected_in_scope_count = df_detected_in_scope.count()
    total_truth_episodes = df_truth_extracted.select("extracted_vehicle_id", "anomaly_type").distinct().count()

    if detected_in_scope_count == 0:
        df_detected_in_scope = df_detected_episodes.join(
            truth_vehicles,
            F.col("episodes.vehicle_id") == F.col("t_veh.extracted_vehicle_id"),
            "inner"
        ).alias("scope_episodes")
        detected_in_scope_count = df_detected_in_scope.count()

    precision = (tp_count / detected_in_scope_count) if detected_in_scope_count > 0 else 0.0
    recall = (tp_count / total_truth_episodes) if total_truth_episodes > 0 else 0.0
    f1_score = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

    eval_schema = StructType([
        StructField("precision", DoubleType(), True),
        StructField("recall", DoubleType(), True),
        StructField("f1_score", DoubleType(), True),
        StructField("true_positives", LongType(), True),
        StructField("total_detected_in_scope", LongType(), True),
        StructField("total_truth_episodes", LongType(), True)
    ])
    
    eval_data = [(
        float(precision), 
        float(recall), 
        float(f1_score), 
        int(tp_count), 
        int(detected_in_scope_count), 
        int(total_truth_episodes)
    )]
    
    df_eval = spark.createDataFrame(eval_data, eval_schema)
    df_eval.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("evaluation_metrics")
    
    display(df_eval)

    print(f"Precision : {precision * 100:.2f}%")
    print(f"Recall    : {recall * 100:.2f}%")
    print(f"F1-Score  : {f1_score * 100:.2f}%")

except Exception as e:
    print(f"Exception during evaluation: {e}")

