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

if spark.catalog.tableExists("realtime_fleet_alerts"):
    spark.sql("TRUNCATE TABLE realtime_fleet_alerts")
    print("Table 'realtime_fleet_alerts' truncated successfully!")

df_bronze_stream = spark.readStream \
    .format("delta") \
    .option("ignoreChanges", "true") \
    .table("bronze_telemetry_v2")

def process_realtime_alerts_microbatch(microBatchDF, batchId):
    if microBatchDF.isEmpty():
        return
    
    df_parsed = microBatchDF.withColumn(
        "parsed_data", F.from_json(F.col("raw"), telemetry_schema)
    ).select(
        F.col("db_id"),              
        F.col("source"),
        F.col("EventEnqueuedUtcTime"),
        F.col("parsed_data.*")
    ).withColumn("event_time", F.col("event_time").cast(TimestampType()))

    df_valid = df_parsed.filter(
        (F.col("source") == "kafka_cdc") &   
        (F.col("event_id").isNotNull()) & 
        (F.col("vehicle_id").isNotNull()) &
        (F.col("event_time").isNotNull())
    )

    if df_valid.isEmpty():
        return

    # -------------------------------------------------------------
    # Real-Time Incidents Rules (Kafka CDC Stream Only):
    # Priority 1: COLD_CHAIN_VIOLATION
    # Priority 2: ENGINE_OVERHEATING
    # Priority 3: CRITICAL_SPEEDING
    # -------------------------------------------------------------
    df_alerts = df_valid.filter(
        (F.col("speed_kmh") > 105) | 
        (F.col("cargo_temp_c").isNotNull() & ((F.col("cargo_temp_c") < 2) | (F.col("cargo_temp_c") > 8))) |
        (F.col("engine_temp_c") > 105)
    ).withColumn(
        "alert_type",
        F.when(
            F.col("cargo_temp_c").isNotNull() & ((F.col("cargo_temp_c") < 2) | (F.col("cargo_temp_c") > 8)), 
            F.lit("COLD_CHAIN_VIOLATION")
        )
        .when(F.col("engine_temp_c") > 105, F.lit("ENGINE_OVERHEATING"))
        .otherwise(F.lit("CRITICAL_SPEEDING"))
    ).withColumn(
        "severity",
        F.when((F.col("speed_kmh") > 110) | (F.col("engine_temp_c") > 115), F.lit("CRITICAL"))
         .when(F.col("speed_kmh") > 105, F.lit("HIGH"))
         .otherwise(F.lit("MEDIUM"))
    ).withColumn(
        "alert_processed_at", F.current_timestamp()
    ).withColumn(
        "event_date", F.to_date(F.col("event_time"))
    ).select(
        "event_id",
        "vehicle_id",
        "trip_id",
        "event_time",
        "speed_kmh",
        "engine_temp_c",
        "cargo_temp_c",
        "latitude",
        "longitude",
        "alert_type",
        "severity",
        "alert_processed_at",
        "event_date"
    )

    if not df_alerts.isEmpty():
        df_alerts.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("event_date") \
            .saveAsTable("realtime_fleet_alerts")

query = df_bronze_stream.writeStream \
    .format("delta") \
    .foreachBatch(process_realtime_alerts_microbatch) \
    .option("checkpointLocation", "Files/checkpoints/realtime_alerts_v8") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
