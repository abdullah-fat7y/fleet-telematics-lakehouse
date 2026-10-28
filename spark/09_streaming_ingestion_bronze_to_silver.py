from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType, BooleanType
)

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

df_bronze_stream = spark.readStream \
    .format("delta") \
    .option("ignoreChanges", "true") \
    .table("bronze_telemetry_v2")

def process_silver_streaming_microbatch(microBatchDF, batchId):
    if microBatchDF.isEmpty():
        return
    
    df_parsed = microBatchDF.withColumn(
        "parsed_data", F.from_json(F.col("raw"), telemetry_schema)
    ).select(
        F.col("db_id"),               
        F.col("source"),               
        F.col("is_valid"),                
        F.col("reject_reason"),           
        F.col("EventEnqueuedUtcTime"),
        F.col("parsed_data.*")
    ).withColumn("event_time", F.col("event_time").cast(TimestampType()))

    validation_condition = (
        F.col("event_id").isNotNull() & 
        F.col("vehicle_id").isNotNull() &
        F.col("event_time").isNotNull() &
        F.col("latitude").isNotNull() &
        F.col("longitude").isNotNull() &
        F.col("speed_kmh").isNotNull()
    )

    df_deduplicated_mb = df_parsed.dropDuplicates(["db_id"])

    df_rejected = df_deduplicated_mb.filter(
        (F.col("is_valid") == False) | (~validation_condition)
    ).withColumn(
        "rejection_reason", 
        F.coalesce(F.col("reject_reason"), F.lit("missing mandatory fields (null keys/coords/speed)"))
    ).withColumn("rejected_at", F.current_timestamp())

    if not df_rejected.isEmpty():
        df_rejected.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("silver_rejected")

    df_valid = df_deduplicated_mb.filter(
        (F.col("is_valid") == True) & validation_condition
    )

    df_valid_clean = df_valid.dropDuplicates(["vehicle_id", "event_time"]) \
                            .withColumn("event_date", F.to_date(F.col("event_time")))

    if spark.catalog.tableExists("silver_telemetry"):
        silver_existing = spark.read.table("silver_telemetry").select("db_id")
        new_valid_records = df_valid_clean.join(
            silver_existing,
            on="db_id",
            how="left_anti"
        )
    else:
        new_valid_records = df_valid_clean

    if not new_valid_records.isEmpty():
        new_valid_records.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("event_date") \
            .saveAsTable("silver_telemetry")

query = df_bronze_stream.writeStream \
    .format("delta") \
    .foreachBatch(process_silver_streaming_microbatch) \
    .option("checkpointLocation", "Files/checkpoints/silver_streaming_full_rules") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

print("write new stream only")
