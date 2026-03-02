from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_timestamp, lit


# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("TRUSTED - Produtos (Snapshot)")
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

raw_base = "/data/raw/ecommerce/produtos"
trusted_base = "/data/trusted/ecommerce/produtos"

raw_path = spark._jvm.org.apache.hadoop.fs.Path(raw_base)
trusted_path = spark._jvm.org.apache.hadoop.fs.Path(trusted_base)

# -----------------------------
# Verificação RAW
# -----------------------------
if not fs.exists(raw_path):
    print("⚠️ RAW produtos não encontrada.")
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
print(f"📅 Snapshot produtos dt={execution_date}")

# -----------------------------
# Leitura RAW (somente última dt)
# -----------------------------
df_raw = spark.read.parquet(f"{raw_base}/dt={execution_date}")

# -----------------------------
# Validações e normalização
# -----------------------------
df_trusted = (
    df_raw
    .filter(col("id_produto").isNotNull())
    .filter(col("id_categoria").isNotNull())
    .filter(col("nome_produto").isNotNull())
    .filter(col("preco") > 0)
    .withColumn("nome_produto", trim(col("nome_produto")))
    .dropDuplicates(["id_produto"])
    .withColumn("trusted_processed_ts", current_timestamp())
    .withColumn("trusted_version", lit(1))
)

# -----------------------------
# LIMPEZA TOTAL (snapshot)
# -----------------------------
if fs.exists(trusted_path):
    fs.delete(trusted_path, True)

# -----------------------------
# Escrita Silver (overwrite total)
# -----------------------------
(
    df_trusted.write
    .mode("overwrite")
    .parquet(trusted_base)
)

# -----------------------------
# Metastore (SEM PARTIÇÃO)
# -----------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS trusted_ecommerce")

spark.sql("""
CREATE TABLE IF NOT EXISTS trusted_ecommerce.produtos (
    id_produto INT,
    id_categoria INT,
    nome_produto STRING,
    descricao STRING,
    preco DECIMAL(12,2),
    ativo BOOLEAN,
    ingestion_ts TIMESTAMP,
    source_system STRING,
    trusted_processed_ts TIMESTAMP,
    trusted_version INT
)
USING PARQUET
LOCATION '/data/trusted/ecommerce/produtos'
""")

print("✅ TRUSTED produtos (snapshot) OK")
spark.stop()
