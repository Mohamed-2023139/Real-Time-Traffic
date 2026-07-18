from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# ==========================================
# Spark Session
# ==========================================

spark = (
    SparkSession.builder
    .appName("SilverMaintenance")
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

SILVER_PATH = "/opt/spark/warehouse/traffic_silver"

print("=" * 60)
print("Starting Silver Maintenance")
print("=" * 60)

# ==========================================
# Read Silver
# ==========================================

df = spark.read.format("delta").load(SILVER_PATH)

count = df.count()
print(f"Records : {count}")

# ==========================================
# Small Files Compaction
# ==========================================

print("\nCompacting small files...")

(
    df.coalesce(1)          # لو البيانات قليلة
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(SILVER_PATH)
)

print("Compaction completed.")

# ==========================================
# Vacuum
# ==========================================

print("\nRunning VACUUM...")

spark.conf.set(
    "spark.databricks.delta.retentionDurationCheck.enabled",
    "false"
)

delta_table = DeltaTable.forPath(spark, SILVER_PATH)

delta_table.vacuum(0)

print("VACUUM completed.")

print("=" * 60)
print("Silver Maintenance Finished Successfully")
print("=" * 60)

spark.stop()