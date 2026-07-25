from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def build_dim_weather():

    spark = (
        SparkSession.builder
        .appName("DimWeather")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
        .getOrCreate()
    )

    silver = (
        spark.read
        .format("delta")
        .load("/warehouse/traffic_silver")
    )

    window = Window.orderBy("weather")

    weather = upper(col("weather"))

    dim_weather = (
        silver
        .select("weather")
        .dropDuplicates()

        .withColumn(
            "weather_group",
            when(weather=="CLEAR","Clear")
            .when(weather.isin("RAIN","STORM"),"Rainy")
            .when(weather=="FOG","Foggy")
            .otherwise("Other")
        )

        .withColumn(
            "visibility",
            when(weather=="CLEAR","High")
            .when(weather=="RAIN","Medium")
            .when(weather=="FOG","Low")
            .otherwise("Very Low")
        )

        .withColumn(
            "road_condition",
            when(weather=="CLEAR","Dry")
            .when(weather=="RAIN","Wet")
            .when(weather=="FOG","Moist")
            .otherwise("Slippery")
        )

        .withColumn(
            "driving_condition",
            when(weather=="CLEAR","Good")
            .when(weather=="FOG","Moderate")
            .otherwise("Poor")
        )

        .withColumn(
            "weather_key",
            row_number().over(window)
        )

        .select(
            "weather_key",
            "weather",
            "weather_group",
            "visibility",
            "road_condition",
            "driving_condition"
        )
    )

    (
        dim_weather.write
        .format("delta")
        .mode("overwrite")
        .save("/warehouse/gold/dim_weather")
    )

    print("Dim Weather Created Successfully")

if __name__ == "__main__":
    build_dim_weather()