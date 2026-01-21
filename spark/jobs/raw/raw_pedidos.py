from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from common.utils import normalize_columns

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("RAW - Pedidos")
    .enableHiveSupport()
    .getOrCreate()
)

# -----------------------------
# HDFS / FileSystem
# -----------------------------
hadoop_conf = spark._jsc.hadoopConfiguration()

hdfs_uri = spark._jvm.java.net.URI("hdfs://namenode:8020")

fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    hdfs_uri,
    hadoop_conf
)


landing_base = "hdfs://namenode:8020/data/landing/ecommerce/pedidos"
landing_path = spark._jvm.org.apache.hadoop.fs.Path(landing_base)

# -----------------------------
# Descoberta automática da data
# -----------------------------
if not fs.exists(landing_path):
    print("⚠️ Nenhuma landing encontrada. Encerrando job.")
    spark.stop()
    exit(0)

status = fs.listStatus(landing_path)

datas_disponiveis = sorted([
    p.getPath().getName().replace("dt=", "")
    for p in status
    if p.isDirectory() and p.getPath().getName().startswith("dt=")
])

if not datas_disponiveis:
    print("⚠️ Nenhuma partição dt encontrada.")
    spark.stop()
    exit(0)

execution_date = datas_disponiveis[-1]

print(f"📅 Data selecionada para processamento: {execution_date}")

# -----------------------------
# Paths
# -----------------------------
input_path = (
    f"hdfs://namenode:8020/data/landing/ecommerce/"
    f"pedidos/dt={execution_date}"
)

output_base_path = (
    "hdfs://namenode:8020/data/raw/ecommerce/pedidos"
)

print(f"📥 Lendo landing: {input_path}")

# -----------------------------
# Leitura Landing
# -----------------------------
df_landing = spark.read.parquet(input_path)

# -----------------------------
# Padronização de colunas
# -----------------------------
df_landing = normalize_columns(df_landing)

# -----------------------------
# Schema enforcement + mapeamento correto
# -----------------------------
df_raw = (
    df_landing
    .select(
        col("id_pedido").cast("int"),
        col("id_cliente").cast("int"),
        col("id_endereco").cast("int"),
        col("data_pedido").cast("timestamp"),
        col("status_pedido").cast("string"),
        col("valor_total").cast("decimal(12,2)")
    )
)

# -----------------------------
# Metadados técnicos
# -----------------------------
df_raw = (
    df_raw
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source_system", lit("ecommerce_mysql"))
    .withColumn("dt", lit(execution_date))
)

# -----------------------------
# Deduplicação técnica
# -----------------------------
df_raw = df_raw.dropDuplicates(["id_pedido"])

print(f"📊 Registros processados: {df_raw.count()}")
print(f"📤 Gravando RAW em {output_base_path}")

# -----------------------------
# Escrita RAW (Parquet)
# -----------------------------

(
    df_raw.write
    .mode("append")
    .partitionBy("dt")
    .parquet(output_base_path)
)

# -----------------------------
# Metastore (Hive / Spark SQL)
# -----------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS raw_ecommerce")

spark.sql("""
CREATE TABLE IF NOT EXISTS raw_ecommerce.pedidos (
    id_pedido INT,
    id_cliente INT,
    id_endereco INT,
    data_pedido TIMESTAMP,
    status_pedido STRING,
    valor_total DECIMAL(12,2),
    ingestion_ts TIMESTAMP,
    source_system STRING,
    dt STRING
)
USING PARQUET
PARTITIONED BY (dt)
LOCATION 'hdfs://namenode:8020/data/raw/ecommerce/pedidos'
""")

spark.sql("MSCK REPAIR TABLE raw_ecommerce.pedidos")


print("✅ RAW pagamentos processada com sucesso")

spark.stop()
