from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =====================================================
# Spark Session
# =====================================================

spark = (
    SparkSession.builder
    .appName("TrafficSilverLayer")
    .master("spark://spark-master:7077")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# Read Bronze Stream
# =====================================================

bronze_df = (
    spark.readStream
    .format("delta")
    .load("/warehouse/traffic_bronze")
)

# =====================================================
# Safe Casting
# =====================================================

typed_df = (
    bronze_df
    .withColumn("speed_int", col("speed").cast("int"))
    .withColumn("event_ts", to_timestamp("event_time"))
    .withColumn("ingestion_time", current_timestamp())
)

# =====================================================
# Data Quality Checks
# =====================================================

validated = (
    typed_df

    # Mandatory fields
    .withColumn("vehicle_valid",
                col("vehicle_id").isNotNull())

    .withColumn("road_valid",
                col("road_id").isNotNull())

    .withColumn("zone_valid",
                col("city_zone").isNotNull())

    .withColumn("speed_valid",
                col("speed_int").between(0, 160))

    .withColumn("weather_valid",
                col("weather").isin(
                    "CLEAR",
                    "RAIN",
                    "FOG",
                    "STORM"
                ))

    .withColumn("congestion_valid",
                col("congestion_level").between(1, 5))

    .withColumn("time_valid",
                col("event_ts").isNotNull())

    .withColumn(
        "future_time_valid",
        col("event_ts") <=
        current_timestamp() + expr("INTERVAL 10 MINUTES")
    )

    .withColumn(
        "json_valid",
        ~col("raw_json").contains("CORRUPTED")
    )
)

# =====================================================
# Overall Validation Flag
# =====================================================

validated = validated.withColumn(
    "is_valid",
    col("vehicle_valid")
    & col("road_valid")
    & col("zone_valid")
    & col("speed_valid")
    & col("weather_valid")
    & col("congestion_valid")
    & col("time_valid")
    & col("future_time_valid")
    & col("json_valid")
)

# =====================================================
# Good Records
# =====================================================

good_records = validated.filter(col("is_valid"))

# =====================================================
# Bad Records (Quarantine)
# =====================================================

bad_records = validated.filter(~col("is_valid"))

# =====================================================
# Handle Late Data
# =====================================================

good_records = good_records.withWatermark(
    "event_ts",
    "15 minutes"
)

# =====================================================
# Remove Duplicates
# =====================================================

deduped = good_records.dropDuplicates(
    ["vehicle_id", "event_ts"]
)

# =====================================================
# Feature Engineering
# =====================================================

silver_df = (
    deduped

    .withColumn("hour", hour("event_ts"))

    .withColumn(
        "peak_flag",
        when(
            (col("hour").between(8, 11))
            | (col("hour").between(17, 20)),
            1
        ).otherwise(0)
    )

    .withColumn(
        "speed_band",
        when(col("speed_int") < 30, "LOW")
        .when(col("speed_int") < 70, "MEDIUM")
        .otherwise("HIGH")
    )
)

# =====================================================
# Write Silver
# =====================================================

silver_query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/warehouse/chk/traffic_silver"
    )
    .option(
        "path",
        "/warehouse/traffic_silver"
    )
    .start()
)

# =====================================================
# Write Quarantine
# =====================================================

quarantine_query = (
    bad_records.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/warehouse/chk/traffic_quarantine"
    )
    .option(
        "path",
        "/warehouse/traffic_quarantine"
    )
    .start()
)

# =====================================================
# Await Termination
# =====================================================

spark.streams.awaitAnyTermination()