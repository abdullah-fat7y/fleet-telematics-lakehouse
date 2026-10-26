#!/usr/bin/env python
# coding: utf-8

# ## 07_bronze_to_silver
# 
# null

# In[2]:


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

df_bronze = spark.read.table("bronze_telemetry_v2")

if last_processed_time:
    df_bronze = df_bronze.filter(col("EventEnqueuedUtcTime") > last_processed_time)

df_parsed = df_bronze.withColumn("parsed_data", from_json(col("raw"), telemetry_schema)) \
                     .select(
                         col("db_id"),               
                         col("source"),               
                         col("is_valid"),                
                         col("reject_reason"),           
                         col("EventEnqueuedUtcTime"),
                         col("parsed_data.*")
                     ) \
                     .withColumn("event_time", col("event_time").cast(TimestampType()))

display(df_parsed.limit(100))


# In[3]:


validation_condition = (
    col("event_id").isNotNull() & 
    col("vehicle_id").isNotNull() &
    col("event_time").isNotNull() &
    col("latitude").isNotNull() &
    col("longitude").isNotNull() &
    col("speed_kmh").isNotNull()
)

df_deduplicated = df_parsed.dropDuplicates(["db_id"])

df_rejected = df_deduplicated.filter(
    (col("is_valid") == False) | (~validation_condition)
).withColumn(
    "rejection_reason", 
    coalesce(col("reject_reason"), lit("missing mandatory fields (null keys/coords/speed)"))
).withColumn("rejected_at", current_timestamp())

df_rejected.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_rejected")
    
df_valid = df_deduplicated.filter(
    (col("is_valid") == True) & validation_condition
)

print(f"Total Parsed: {df_parsed.count()}")
print(f"Total Rejected: {df_rejected.count()}")
print(f"Total Valid: {df_valid.count()}")


# In[5]:


df_deduplicated = df_valid.dropDuplicates(["db_id"])

valid_count = df_valid.count()
deduped_count = df_deduplicated.count()
duplicates_removed = valid_count - deduped_count

print(f"Valid Records Before Dedup: {valid_count}")
print(f"Duplicates Removed: {duplicates_removed}")
print(f"Clean Deduped Records: {deduped_count}")
from delta.tables import DeltaTable

df_silver_clean = df_valid.withColumn("event_date", to_date(col("event_time")))

silver_table_name = "silver_telemetry"
# for inc load
if spark.catalog.tableExists(silver_table_name):
    delta_silver = DeltaTable.forName(spark, silver_table_name)
    
    delta_silver.alias("target") \
        .merge(
            df_silver_clean.alias("source"),
            "target.db_id = source.db_id"
        ) \
        .whenNotMatchedInsertAll() \
        .execute()
    print("MERGE executed successfully in Silver layer!")
else:
    df_silver_clean.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("event_date") \
        .saveAsTable(silver_table_name)
    print("silver_telemetry created for the first time!")

