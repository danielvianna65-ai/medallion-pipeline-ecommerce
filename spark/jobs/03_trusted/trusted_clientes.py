from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_timestamp, lit


# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("TRUSTED - Clientes (Snapshot)")
    .enableHiveSupport()
    .config(
            "spark.sql.warehouse.dir",
            "hdfs://namenode:8020/user/hive/warehouse"
        )
    .getOrCreate()
)

# -----------------------------
# HDFS / FileSystem
# -----------------------------
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.defaultFS", "hdfs://namenode:8020")

fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

raw_base = "/data/raw/ecommerce/clientes"
trusted_base = "/data/trusted/ecommerce/clientes"

raw_path = spark._jvm.org.apache.hadoop.fs.Path(raw_base)
trusted_path = spark._jvm.org.apache.hadoop.fs.Path(trusted_base)

# -----------------------------
# Verificação RAW
# -----------------------------
if not fs.exists(raw_path):
    print("⚠️ RAW clientes não encontrada.")
    spark.stop()
    exit(0)

# -----------------------------
# Descobrir última dt disponível
# -----------------------------
status = fs.listStatus(raw_path)
datas = sorted([
    p.getPath().getName().replace("dt=", "")
    for p in status if p.isDirectory() and p.getPath().getName().startswith("dt=")
])

execution_date = datas[-1]
print(f"📅 Snapshot clientes dt={execution_date}")

# -----------------------------
# Leitura RAW (somente última dt)
# -----------------------------
df_raw = spark.read.parquet(f"{raw_base}/dt={execution_date}")

# -----------------------------
# Validações e normalização
# -----------------------------
df_trusted = (
    df_raw
    .filter(col("id_cliente").isNotNull())
    .filter(col("nome").isNotNull())
    .filter(trim(col("nome")) != "")
    .dropDuplicates(["id_cliente"])
    .withColumn("nome", trim(col("nome")))
    .withColumn("trusted_processed_ts", current_timestamp())
    .withColumn("trusted_version", lit(1))
)

# -----------------------------
# LIMPEZA TOTAL (snapshot)
# -----------------------------
if fs.exists(trusted_path):
    fs.delete(trusted_path, True)

# -----------------------------
# Escrita Trusted (overwrite total)
# -----------------------------
(
    df_trusted.write
    .mode("overwrite")
    .parquet(trusted_base)
)

# -----------------------------
# Metastore
# -----------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS trusted_ecommerce")

spark.sql("""
CREATE TABLE IF NOT EXISTS trusted_ecommerce.clientes (
    id_cliente INT,
    nome STRING,
    email STRING,
    telefone STRING,
    data_cadastro TIMESTAMP,
    ingestion_ts TIMESTAMP,
    source_system STRING,
    trusted_processed_ts TIMESTAMP,
    trusted_version INT
)
USING PARQUET
LOCATION '/data/trusted/ecommerce/clientes'
""")

print("✅ Trusted clientes (snapshot) OK")
spark.stop()
