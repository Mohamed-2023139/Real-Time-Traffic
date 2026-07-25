from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def build_dim_road():

    spark = (
        SparkSession.builder
        .appName("DimRoad")
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

    window = Window.orderBy("road_id")

    road = upper(col("road_id"))

    dim_road = (
        silver
        .select("road_id")
        .dropDuplicates()

        .withColumn(
            "road_type",
            when(road.isin("R100","R200"),"Highway")
            .when(road.isin("R300","R400"),"Main Road")
            .otherwise("Local Road")
        )

        .withColumn(
            "road_class",
            when(road.isin("R100","R200"),"Primary")
            .when(road.isin("R300","R400"),"Secondary")
            .otherwise("Local")
        )

        .withColumn(
            "speed_limit",
            when(road.isin("R100","R200"),100)
            .when(road.isin("R300","R400"),80)
            .otherwise(60)
        )

        .withColumn(
            "lanes",
            when(road.isin("R100","R200"),4)
            .when(road.isin("R300","R400"),3)
            .otherwise(2)
        )

        .withColumn(
            "toll_road",
            when(road=="R100","Yes")
            .otherwise("No")
        )

        .withColumn(
            "road_surface",
            when(road.isin("R100","R200"),"Asphalt")
            .otherwise("Concrete")
        )

        .withColumn(
            "road_key",
            row_number().over(window)
        )

        .select(
            "road_key",
            "road_id",
            "road_type",
            "road_class",
            "speed_limit",
            "lanes",
            "toll_road",
            "road_surface"
        )
    )

    (
        dim_road.write
        .format("delta")
        .mode("overwrite")
        .save("/warehouse/gold/dim_road")
    )

    print("Dim Road Created Successfully")

if __name__ == "__main__":
    build_dim_road()