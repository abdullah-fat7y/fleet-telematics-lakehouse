#!/usr/bin/env python
# coding: utf-8

# ## 09_streaming_ingestion_bronze_to_silver
# 
# null

# In[6]:


from delta.tables import DeltaTable
from pyspark.sql import functions as F

df_bronze_stream = spark.readStream \
    .format("delta") \
    .option("ignoreChanges", "true") \
    .table("bronze_telemetry_v2")

df_silver_stream = df_bronze_stream.filter(F.col("is_valid") == True)

def insert_only_new(microBatchDF, batchId):
    clean_micro_batch = microBatchDF.dropDuplicates(["db_id"])
    
    silver_existing = spark.read.table("silver_telemetry").select("db_id")
    
    new_records_only = clean_micro_batch.join(
        silver_existing,
        on="db_id",
        how="left_anti"
    )
    
    if not new_records_only.isEmpty():
        new_records_only.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("silver_telemetry")

query = df_silver_stream.writeStream \
    .format("delta") \
    .foreachBatch(insert_only_new) \
    .option("checkpointLocation", "Files/checkpoints/silver_insert_new_only") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

print("Only new records were inserted into Silver successfully!")

