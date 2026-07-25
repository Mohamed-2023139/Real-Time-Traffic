from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime


# =====================================================
# Spark Session
# =====================================================

spark = (
    SparkSession.builder
    .appName("SilverQualityReport")
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
# Read Silver Layer
# =====================================================

silver_df = (
    spark.read
    .format("delta")
    .load("/warehouse/traffic_silver")
)


# =====================================================
# Quality Metrics
# =====================================================

total_records = silver_df.count()


# Null Checks

null_vehicle_ids = (
    silver_df
    .filter(col("vehicle_id").isNull())
    .count()
)


null_road_ids = (
    silver_df
    .filter(col("road_id").isNull())
    .count()
)


null_zone = (
    silver_df
    .filter(col("city_zone").isNull())
    .count()
)


# Speed Validation

invalid_speed = (
    silver_df
    .filter(
        (col("speed_int") < 0) |
        (col("speed_int") > 160)
    )
    .count()
)


# Weather Validation

invalid_weather = (
    silver_df
    .filter(
        ~col("weather").isin(
            "CLEAR",
            "RAIN",
            "FOG",
            "STORM"
        )
    )
    .count()
)


# Future Events

future_events = (
    silver_df
    .filter(
        col("event_ts") >
        current_timestamp() + expr("INTERVAL 10 MINUTES")
    )
    .count()
)


# =====================================================
# Duplicate Detection
# =====================================================

duplicates = (
    silver_df
    .groupBy(
        "vehicle_id",
        "event_ts"
    )
    .count()
    .filter(col("count") > 1)
    .count()
)



# =====================================================
# Quality Status
# =====================================================

if (
    null_vehicle_ids == 0
    and null_road_ids == 0
    and null_zone == 0
    and invalid_speed == 0
    and invalid_weather == 0
    and future_events == 0
    and duplicates == 0
):

    status = "PASS"

else:

    status = "FAIL"



# =====================================================
# Generate Report
# =====================================================

report = f"""

=================================================
              SILVER DATA QUALITY REPORT
=================================================

Execution Time .......... {datetime.now()}


Total Records ........... {total_records}


Null Vehicle IDs ........ {null_vehicle_ids}

Null Road IDs ........... {null_road_ids}

Null Zones .............. {null_zone}


Duplicate Records ....... {duplicates}


Invalid Speed ........... {invalid_speed}

Invalid Weather ......... {invalid_weather}


Future Events ........... {future_events}



Overall Status .......... {status}


=================================================

"""


print(report)



# =====================================================
# Save Quality Report
# =====================================================

quality_df = spark.createDataFrame(
    [
        (
            datetime.now(),
            total_records,
            null_vehicle_ids,
            null_road_ids,
            null_zone,
            duplicates,
            invalid_speed,
            invalid_weather,
            future_events,
            status
        )
    ],
    [
        "execution_time",
        "total_records",
        "null_vehicle_ids",
        "null_road_ids",
        "null_zone",
        "duplicate_records",
        "invalid_speed",
        "invalid_weather",
        "future_events",
        "status"
    ]
)



(
    quality_df
    .write
    .format("delta")
    .mode("append")
    .save(
        "/warehouse/silver_quality_report"
    )
)


spark.stop()