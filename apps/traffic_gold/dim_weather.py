from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def build_dim_weather():

    spark = (
        SparkSession.builder
        .appName("DimWeather")
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

    silver = (
        spark.read
        .format("delta")
        .load("/opt/spark/warehouse/traffic_silver")
    )

    window = Window.orderBy("weather")

    dim_weather = (
        silver
        .select("weather")
        .dropDuplicates()

        .withColumn(
            "weather_group",
            when(col("weather").isin("Sunny", "Clear"), "Clear")
            .when(col("weather").isin("Rain", "Storm"), "Rainy")
            .when(col("weather").isin("Fog", "Mist"), "Foggy")
            .otherwise("Other")
        )

        .withColumn(
            "weather_key",
            row_number().over(window)
        )

        .select(
            "weather_key",
            "weather",
            "weather_group"
        )
    )

    (
        dim_weather.write
        .format("delta")
        .mode("overwrite")
        .save("/opt/spark/warehouse/gold/dim_weather")
    )

    print("✅ Dim_Weather Created Successfully")


if __name__ == "__main__":
    build_dim_weather()