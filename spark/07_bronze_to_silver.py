#!/usr/bin/env python
# coding: utf-8

# ## 07_bronze_to_silver
# 
# null

# In[1]:


from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, when, 
    to_date, expr, broadcast, coalesce, lag, lead, 
    sum, avg, count, abs, round, timestamp_seconds, concat_ws, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType, LongType, BooleanType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

telemetry_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("engine_temp_c", DoubleType(), True),
    StructField("fuel_rate_lph", DoubleType(), True),
    StructField("odometer_km", DoubleType(), True),
    StructField("cargo_temp_c", DoubleType(), True)
])

silver_table_exists = spark.catalog.tableExists("silver_telemetry")

if silver_table_exists:
    last_processed_time = spark.table("silver_telemetry").select(spark_max("EventEnqueuedUtcTime")).collect()[0][0]
else:
    last_processed_time = None

df_bronze = spark.read.table("bronze_telemetry")

if last_processed_time:
    df_bronze = df_bronze.filter(col("EventEnqueuedUtcTime") > last_processed_time)

df_parsed = df_bronze.withColumn("parsed_data", from_json(col("raw"), telemetry_schema)) \
                     .select("EventEnqueuedUtcTime", "parsed_data.*") \
                     .withColumn("event_time", col("event_time").cast(TimestampType()))

display(df_parsed.limit(100)) 
# i just enforced schema here for telemetry (raw) and parsed  raw json column and ensure that schema that i enforced is passed and converted event time to time_stamp


# In[2]:


validation_condition = (
    col("event_id").isNotNull() & 
    col("vehicle_id").isNotNull() &
    col("event_time").isNotNull() &
    col("latitude").isNotNull() &
    col("longitude").isNotNull() &
    col("speed_kmh").isNotNull()
)

df_rejected = df_parsed.filter(~validation_condition) \
    .withColumn("rejection_reason", lit("missing mandatory fields (null keys/coords/speed)")) \
    .withColumn("rejected_at", current_timestamp())

df_rejected.write.format("delta").mode("append").saveAsTable("silver_rejected")

df_valid = df_parsed.filter(validation_condition)

print(f"Total Parsed: {df_parsed.count()}")
print(f"Total Rejected: {df_rejected.count()}")
print(f"Total Valid: {df_valid.count()}") 

# here i just defined a validation condition to determine on which methodology i will follow to reject and added a rejection time column and a column for the reason of rejection at the end i wrote all rejected records
# in a separate table in lakehouse


# In[3]:


df_deduplicated = df_valid.dropDuplicates(["vehicle_id", "event_time"])

valid_count = df_valid.count()
deduped_count = df_deduplicated.count()
duplicates_removed = valid_count - deduped_count

print(f"Valid Records Before Dedup: {valid_count}")
print(f"Duplicates Removed: {duplicates_removed}")
print(f"Clean Deduped Records: {deduped_count}") 

# here i just ensure that every vehicle has only one read by dropping duplicates by event time and vehicle id because it is impossiple for any vehicle to have two reads at the same moment  and just some calculations after that

df_silver_clean = df_deduplicated.withColumn("event_date", to_date("event_time"))

if silver_table_exists:
    silver_table = DeltaTable.forName(spark, "silver_telemetry")
    silver_table.alias("target").merge(
        df_silver_clean.alias("source"),
        "target.vehicle_id = source.vehicle_id AND target.event_time = source.event_time"
    ).whenNotMatchedInsertAll().execute()
else:
    df_silver_clean.write.format("delta") \
        .mode("append") \
        .partitionBy("event_date") \
        .saveAsTable("silver_telemetry")

print("silver_telemetry successfully written to lakehouse")

