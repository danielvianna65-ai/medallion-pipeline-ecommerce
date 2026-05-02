# =====================================================
# IMPORTS
# =====================================================
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType
from pyspark.sql.functions import current_date

# =====================================================
# Config
# =====================================================
table = "clientes_enrichment"

# =====================================================
# Paths
# =====================================================
source_path = f"/data/reference/{table}.csv"
landing_path = f"/data/01_landing/ecommerce/{table}"

# =====================================================
# Spark Session
# =====================================================
spark = (
    SparkSession.builder
    .appName("landing_clientes_enrichment")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .getOrCreate()
)

# =====================================================
# READ
# =====================================================
print(f"[LANDING][|{table}] Source: {source_path}")
print(f"[LANDING][{table}] Target: {landing_path}")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("mode", "FAILFAST")
    .csv(source_path)
)

# =====================================================
# PARTITION BY DT
# =====================================================
df = df.withColumn("dt", current_date())

# =====================================================
# WRITE
# =====================================================
print(f"[LANDING][{table}] Gravando")
(
    df.write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet(landing_path)
)

spark.stop()