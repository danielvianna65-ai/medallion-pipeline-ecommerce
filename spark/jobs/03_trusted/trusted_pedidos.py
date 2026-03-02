from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, date_format

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("trusted - Pedidos")
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
raw_pedidos_path = "hdfs://namenode:8020/data/raw/ecommerce/pedidos"
trusted_pedidos_path = "hdfs://namenode:8020/data/trusted/ecommerce/pedidos"
trusted_clientes_path = "hdfs://namenode:8020/data/trusted/ecommerce/clientes"

# -----------------------------
# Leitura RAW
# -----------------------------
df_pedidos_raw = spark.read.parquet(raw_pedidos_path)
df_clientes = spark.read.parquet(trusted_clientes_path).select("id_cliente").dropDuplicates()

# -----------------------------
# Validações estruturais
# -----------------------------
df_valid = (
    df_pedidos_raw
    .filter(col("id_pedido").isNotNull())
    .filter(col("id_cliente").isNotNull())
    .filter(col("data_pedido").isNotNull())
)

# -----------------------------
# Validações de negócio
# -----------------------------
df_valid = (
    df_valid
    .filter(col("valor_total") > 0)
    .filter(col("status_pedido").isin(
        "CRIADO", "PAGO", "CANCELADO", "ENVIADO"
    ))
)

# -----------------------------
# Deduplicação
# -----------------------------
df_valid = df_valid.dropDuplicates(["id_pedido"])

# -----------------------------
# Integridade referencial
# -----------------------------
df_valid = (
    df_valid
    .join(df_clientes, on="id_cliente", how="inner")
)

# -----------------------------
# Metadados Silver
# -----------------------------
df_trusted = (
    df_valid
    .withColumn("trusted_ts", current_timestamp())
    .withColumn("trusted_version", lit(1))
    .withColumn("dt",
    date_format(col("data_pedido"), "yyyy-MM-dd")
    )
)

# -----------------------------
# Escrita Silver
# -----------------------------
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df_trusted
    .write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet(trusted_pedidos_path)
)

# -----------------------------
# Metastore
# -----------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS trusted_ecommerce")

spark.sql("""
CREATE TABLE IF NOT EXISTS trusted_ecommerce.pedidos (
    id_pedido INT,
    id_cliente INT,
    id_endereco INT,
    data_pedido TIMESTAMP,
    status_pedido STRING,
    valor_total DECIMAL(12,2),
    ingestion_ts TIMESTAMP,
    source_system STRING,
    dt STRING,
    trusted_ts TIMESTAMP,
    trusted_version INT
)
USING PARQUET
PARTITIONED BY (dt)
LOCATION 'hdfs://namenode:8020/data/trusted/ecommerce/pedidos'
""")

spark.sql("MSCK REPAIR TABLE trusted_ecommerce.pedidos")

print("✅ Trusted pedidos processada com sucesso")

spark.stop()
