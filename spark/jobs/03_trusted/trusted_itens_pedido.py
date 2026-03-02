from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, round, date_format

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("TRUSTED - Itens Pedido")
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
raw_path = "hdfs://namenode:8020/data/raw/ecommerce/itens_pedido"
trusted_path = "hdfs://namenode:8020/data/trusted/ecommerce/itens_pedido"
pedidos_trusted_path = "hdfs://namenode:8020/data/trusted/ecommerce/pedidos"

# -----------------------------
# Leitura RAW + Pedidos Trusted (para data)
# -----------------------------
df_raw = spark.read.parquet(raw_path)

df_pedidos = (
    spark.read.parquet(pedidos_trusted_path)
    .select("id_pedido", "data_pedido")
    .dropDuplicates()
)

# -----------------------------
# Validações estruturais + negócio
# -----------------------------
df_valid = (
    df_raw
    .filter(col("id_item_pedido").isNotNull())
    .filter(col("id_pedido").isNotNull())
    .filter(col("id_produto").isNotNull())
    .filter(col("quantidade") > 0)
    .filter(col("preco_unitario") > 0)
)

# -----------------------------
# Join com pedidos (herda data_pedido)
# -----------------------------
df_valid = (
    df_valid
    .join(df_pedidos, on="id_pedido", how="inner")
    .filter(col("data_pedido").isNotNull())
)

# -----------------------------
# Regra de negócio
# -----------------------------
df_valid = df_valid.withColumn(
    "valor_total_item",
    round(col("quantidade") * col("preco_unitario"), 2)
)

# -----------------------------
# Deduplicação
# -----------------------------
df_valid = df_valid.dropDuplicates(["id_item_pedido"])

# -----------------------------
# Metadados Trusted + partição diária
# -----------------------------
df_trusted = (
    df_valid
    .withColumn("trusted_ts", current_timestamp())
    .withColumn("trusted_version", lit(1))
    .withColumn("dt", date_format(col("data_pedido"), "yyyy-MM-dd"))
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
CREATE TABLE IF NOT EXISTS trusted_ecommerce.itens_pedido (
    id_item_pedido INT,
    id_pedido INT,
    id_produto INT,
    quantidade INT,
    preco_unitario DECIMAL(12,2),
    valor_total_item DECIMAL(12,2),
    data_pedido TIMESTAMP,
    ingestion_ts TIMESTAMP,
    source_system STRING,
    dt STRING,
    trusted_ts TIMESTAMP,
    trusted_version INT
)
USING PARQUET
PARTITIONED BY (dt)
LOCATION 'hdfs://namenode:8020/data/trusted/ecommerce/itens_pedido'
""")

spark.sql("MSCK REPAIR TABLE trusted_ecommerce.itens_pedido")

print("✅ TRUSTED itens_pedido processada com sucesso")

spark.stop()
