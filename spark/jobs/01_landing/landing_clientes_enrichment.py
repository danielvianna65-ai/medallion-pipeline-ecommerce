# =====================================================
# IMPORTS
# =====================================================
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType
from pyspark.sql.functions import current_date

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
# Paths
# =====================================================
source_path = "/data/reference/clientes_enrichment.csv"
landing_path = "/data/01_landing/ecommerce/clientes_enrichment"

# =====================================================
# READ
# =====================================================
print(f"[LANDING][INFO] Source: {source_path}")
print(f"[LANDING][INFO] Target: {landing_path}")

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
print("[LANDING][INFO] Gravando")
(
    df.write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet(landing_path)
)

spark.stop()