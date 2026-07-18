from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def build_dim_road():

    spark = (
        SparkSession.builder
        .appName("DimRoad")
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

    window = Window.orderBy("road_id")

    dim_road = (
        silver
        .select("road_id")
        .dropDuplicates()

        .withColumn(
            "road_type",
            when(col("road_id").isin("R100", "R200"), "Highway")
            .otherwise("City Road")
        )

        .withColumn(
            "speed_limit",
            when(col("road_id").isin("R100", "R200"), 100)
            .otherwise(60)
        )

        .withColumn(
            "road_key",
            row_number().over(window)
        )

        .select(
            "road_key",
            "road_id",
            "road_type",
            "speed_limit"
        )
    )

    (
        dim_road.write
        .format("delta")
        .mode("overwrite")
        .save("/opt/spark/warehouse/gold/dim_road")
    )

    print("Dim_Road Created Successfully")


if __name__ == "__main__":
    build_dim_road()