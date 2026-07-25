from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ==========================
# Spark Session
# ==========================

spark = (
    SparkSession.builder
    .appName("GoldValidation")
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

# ==========================
# Read Gold Tables
# ==========================

fact = spark.read.format("delta").load(
    "/warehouse/gold/fact_traffic"
)

dim_road = spark.read.format("delta").load(
    "/warehouse/gold/dim_road"
)

dim_zone = spark.read.format("delta").load(
    "/warehouse/gold/dim_zone"
)

dim_weather = spark.read.format("delta").load(
    "/warehouse/gold/dim_weather"
)

dim_date = spark.read.format("delta").load(
    "/warehouse/gold/dim_date"
)

# ==========================
# Row Count
# ==========================

print("=" * 50)
print("ROW COUNTS")
print("=" * 50)

print(f"Fact Traffic : {fact.count()}")
print(f"Dim Road     : {dim_road.count()}")
print(f"Dim Zone     : {dim_zone.count()}")
print(f"Dim Weather  : {dim_weather.count()}")
print(f"Dim Date     : {dim_date.count()}")

# ==========================
# Duplicate PK Check
# ==========================

print("\n" + "=" * 50)
print("PRIMARY KEY VALIDATION")
print("=" * 50)

tables = [
    (fact, "traffic_key", "Fact"),
    (dim_road, "road_key", "Dim Road"),
    (dim_zone, "zone_key", "Dim Zone"),
    (dim_weather, "weather_key", "Dim Weather"),
    (dim_date, "date_key", "Dim Date")
]

for df, key, name in tables:

    duplicates = (
        df.groupBy(key)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print(f"{name}: Duplicate {key} = {duplicates}")

# ==========================
# Null Foreign Keys
# ==========================

print("\n" + "=" * 50)
print("FOREIGN KEY VALIDATION")
print("=" * 50)

fk_columns = [
    "road_key",
    "zone_key",
    "weather_key",
    "date_key"
]

for fk in fk_columns:

    missing = fact.filter(col(fk).isNull()).count()

    print(f"{fk}: NULL = {missing}")

print("\nGold Validation Finished Successfully.")