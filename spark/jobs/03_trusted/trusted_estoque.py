from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, date_format

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("TRUSTED - Estoque")
    .enableHiveSupport()
    .config(
            "spark.sql.warehouse.dir",
            "hdfs://namenode:8020/user/hive/warehouse"
        )
    .getOrCreate()
)

# -----------------------------
# Paths
# -----------------------------
raw_path = "hdfs://namenode:8020/data/raw/ecommerce/estoque"
trusted_path = "hdfs://namenode:8020/data/trusted/ecommerce/estoque"

# -----------------------------
# Leitura RAW
# -----------------------------
df_raw = spark.read.parquet(raw_path)

# -----------------------------
# Validações estruturais + negócio
# -----------------------------
df_valid = (
    df_raw
    .filter(col("id_estoque").isNotNull())
    .filter(col("id_produto").isNotNull())
    .filter(col("quantidade_disponivel").isNotNull())
    .filter(col("quantidade_disponivel") >= 0)
    .filter(col("data_atualizacao").isNotNull())
)

# -----------------------------
# Deduplicação
# -----------------------------
df_valid = df_valid.dropDuplicates(["id_estoque"])

# -----------------------------
# Metadados Trusted + partição diária
# -----------------------------
df_trusted = (
    df_valid
    .withColumn("trusted_ts", current_timestamp())
    .withColumn("trusted_version", lit(1))
    .withColumn("dt", date_format(col("data_atualizacao"), "yyyy-MM-dd"))
)

# -----------------------------
# Escrita Trusted (ingestão diária)
# -----------------------------
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df_trusted
    .write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet(trusted_path)
)

# -----------------------------
# Metastore
# -----------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS trusted_ecommerce")

spark.sql("""
CREATE TABLE IF NOT EXISTS trusted_ecommerce.estoque (
    id_estoque INT,
    id_produto INT,
    quantidade_disponivel INT,
    data_atualizacao TIMESTAMP,
    ingestion_ts TIMESTAMP,
    source_system STRING,
    dt STRING,
    trusted_ts TIMESTAMP,
    trusted_version INT
)
USING PARQUET
PARTITIONED BY (dt)
LOCATION 'hdfs://namenode:8020/data/trusted/ecommerce/estoque'
""")

spark.sql("MSCK REPAIR TABLE trusted_ecommerce.estoque")

print("✅ TRUSTED estoque processada com sucesso")

spark.stop()
