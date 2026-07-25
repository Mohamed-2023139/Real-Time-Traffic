from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("FactTraffic")
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


def build_fact_traffic():

    # ==========================
    # Read Silver
    # ==========================

    silver = (
        spark.read
        .format("delta")
        .load("/warehouse/traffic_silver")
    )

    # ==========================
    # Read Dimensions
    # ==========================

    dim_date = (
        spark.read
        .format("delta")
        .load("/warehouse/gold/dim_date")
    )

    dim_road = (
        spark.read
        .format("delta")
        .load("/warehouse/gold/dim_road")
    )

    dim_zone = (
        spark.read
        .format("delta")
        .load("/warehouse/gold/dim_zone")
    )

    dim_weather = (
        spark.read
        .format("delta")
        .load("/warehouse/gold/dim_weather")
    )

    # ==========================
    # Prepare Silver
    # ==========================

    silver = silver.withColumn(
        "event_date",
        to_date("event_ts")
    )

    # ==========================
    # Lookup Dimension Keys
    # ==========================

    fact = (
        silver

        .join(
            dim_road.select("road_key", "road_id"),
            "road_id",
            "left"
        )

        .join(
            dim_zone.select("zone_key", "city_zone"),
            "city_zone",
            "left"
        )

        .join(
            dim_weather.select("weather_key", "weather"),
            "weather",
            "left"
        )

        .join(
            dim_date.select(
                "date_key",
                "event_date",
                "hour"
            ),
            ["event_date", "hour"],
            "left"
        )
    )

    # ==========================
    # Generate Surrogate Key
    # ==========================

    window = Window.orderBy(
        "event_ts",
        "vehicle_id"
    )

    fact = (
        fact

        .withColumn(
            "traffic_key",
            row_number().over(window)
        )

        .select(
            "traffic_key",
            "road_key",
            "zone_key",
            "date_key",
            "weather_key",
            "vehicle_id",
            "speed_int",
            "congestion_level",
            "peak_flag",
            "speed_band"
        )
    )

    # ==========================
    # Write Fact
    # ==========================

    (
        fact.write
        .format("delta")
        .mode("overwrite")
        .save("/warehouse/gold/fact_traffic")
    )

    print("Fact Traffic Created Successfully")


if __name__ == "__main__":
    build_fact_traffic()