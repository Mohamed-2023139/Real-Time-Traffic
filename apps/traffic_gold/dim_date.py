from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("DimDate")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)


def build_dim_date():

    silver = (
        spark.read
        .format("delta")
        .load("/warehouse/traffic_silver")
    )

    window = Window.orderBy("event_date")

    dim_date = (
        silver

        .withColumn("event_date", to_date("event_ts"))

        .select("event_date", "hour")
        .dropDuplicates()

        .withColumn("day", dayofmonth("event_date"))
        .withColumn("month", month("event_date"))
        .withColumn("month_name", date_format("event_date", "MMMM"))
        .withColumn("quarter", quarter("event_date"))
        .withColumn("year", year("event_date"))
        .withColumn("day_name", date_format("event_date", "EEEE"))

        .withColumn(
            "weekend_flag",
            when(dayofweek("event_date").isin(1, 7), "Yes")
            .otherwise("No")
        )

        .withColumn(
            "date_key",
            row_number().over(window)
        )

        .select(
            "date_key",
            "event_date",
            "day",
            "month",
            "month_name",
            "quarter",
            "year",
            "hour",
            "weekend_flag",
            "day_name"
        )
    )

    (
        dim_date.write
        .format("delta")
        .mode("overwrite")
        .save("/warehouse/gold/dim_date")
    )

    print("Dim Date Created Successfully")


if __name__ == "__main__":
    build_dim_date()