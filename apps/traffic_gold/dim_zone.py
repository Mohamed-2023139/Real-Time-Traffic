from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def build_dim_zone():

    spark = (
        SparkSession.builder
        .appName("DimZone")
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

    window = Window.orderBy("city_zone")

    zone = upper(col("city_zone"))

    dim_zone = (
        silver
        .select("city_zone")
        .dropDuplicates()

        .withColumn(
            "zone_type",
            when(zone.isin("CBD","DOWNTOWN"),"Commercial")
            .when(zone.isin("AIRPORT","TRAINSTATION"),"Transit Hub")
            .when(zone=="TECHPARK","Technology")
            .when(zone=="INDUSTRIAL","Industrial")
            .when(zone=="UNIVERSITY","Education")
            .otherwise("Residential")
        )

        .withColumn(
            "traffic_risk",
            when(zone.isin("CBD","AIRPORT"),"High")
            .when(zone.isin("TECHPARK","TRAINSTATION"),"Medium")
            .otherwise("Low")
        )

        .withColumn(
            "population_density",
            when(zone.isin("CBD","DOWNTOWN"),"High")
            .when(zone.isin("TECHPARK","UNIVERSITY"),"Medium")
            .otherwise("Low")
        )

        .withColumn(
            "business_activity",
            when(zone.isin("CBD","DOWNTOWN"),"High")
            .when(zone=="TECHPARK","Medium")
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
            "traffic_risk",
            "population_density",
            "business_activity"
        )
    )

    (
        dim_zone.write
        .format("delta")
        .mode("overwrite")
        .save("/warehouse/gold/dim_zone")
    )

    print("Dim Zone Created Successfully")

if __name__ == "__main__":
    build_dim_zone()