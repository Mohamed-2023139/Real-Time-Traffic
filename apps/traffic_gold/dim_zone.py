from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("DimZone")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)


def build_dim_zone():

    silver = (
        spark.read
        .format("delta")
        .load("/opt/spark/warehouse/traffic_silver")
    )

    window = Window.orderBy("city_zone")

    dim_zone = (
        silver
        .select("city_zone")
        .dropDuplicates()

        .withColumn(
            "zone_type",
            when(col("city_zone") == "CBD", "Commercial")
            .when(col("city_zone") == "TECHPARK", "IT Hub")
            .when(col("city_zone").isin("AIRPORT", "TRAINSTATION"), "Transit Hub")
            .otherwise("Residential")
        )

        .withColumn(
            "traffic_risk",
            when(col("city_zone").isin("CBD", "AIRPORT", "TRAINSTATION"), "High")
            .when(col("city_zone") == "TECHPARK", "Medium")
            .otherwise("Low")
        )

        .withColumn(
            "zone_key",
            row_number().over(window)
        )

        .select(
            "zone_key",
            "city_zone",
            "zone_type",
            "traffic_risk"
        )
    )

    (
        dim_zone.write
        .format("delta")
        .mode("overwrite")
        .save("/opt/spark/warehouse/gold/dim_zone")
    )

    print("Dim Zone Created Successfully")


if __name__ == "__main__":
    build_dim_zone()