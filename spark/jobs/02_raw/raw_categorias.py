from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import argparse

# =====================================================
# Args Airflow
# =====================================================
parser = argparse.ArgumentParser()

parser.add_argument("--execution_date", required=True)
parser.add_argument("--landing_base", required=True)
parser.add_argument("--raw_base", required=True)

args = parser.parse_args()

# =====================================================
# 🔥 CONFIGURAR POR TABELA
# =====================================================
TABLE = "categorias"
PRIMARY_KEY = "id_categoria"
WATERMARK_COL = "data_transacao"

landing_path = f"{args.landing_base}/{TABLE}"
raw_path = f"{args.raw_base}/{TABLE}"

# =====================================================
# Spark Session Delta
# =====================================================
spark = (
    SparkSession.builder
    .appName(f"raw_incremental_{TABLE}")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

print(f"[RAW] Tabela: {TABLE}")
print(f"[RAW] Execution date: {args.execution_date}")

# =====================================================
# Bootstrap + Auto Backfill RAW (CDC-aware)
# =====================================================
is_bootstrap = not DeltaTable.isDeltaTable(spark, raw_path)

# -----------------------------------------------------
# 🔥 LISTAR PARTIÇÕES LANDING VIA HDFS (SEM SCAN)
# -----------------------------------------------------
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

landing_path_j = spark._jvm.org.apache.hadoop.fs.Path(landing_path)
status = fs.listStatus(landing_path_j)

landing_dt_list = []

for s in status:
    name = s.getPath().getName()
    if name.startswith("dt="):
        landing_dt_list.append(name.replace("dt=", ""))

landing_dt_df = spark.createDataFrame([(d,) for d in landing_dt_list], ["dt"])

# -----------------------------------------------------
# BOOTSTRAP FULL
# -----------------------------------------------------
if is_bootstrap:

    print("[RAW] Bootstrap FULL → lendo histórico completo da LANDING")

    df_inc = spark.read.parquet(landing_path)

# -----------------------------------------------------
# AUTO BACKFILL + CDC SAFE
# -----------------------------------------------------
else:

    print("[RAW] Detectando backlog automaticamente (CDC-aware)...")

    raw_dt_df = (
        spark.read
        .format("delta")
        .load(raw_path)
        .select("dt")
        .distinct()
    )

    # dt ainda não processados
    backlog_dt = landing_dt_df.join(raw_dt_df, ["dt"], "left_anti")

    # 🔥 SEMPRE incluir execution_date
    exec_dt_df = spark.createDataFrame([(args.execution_date,)], ["dt"])

    dt_to_process = (
        backlog_dt.union(exec_dt_df)
        .dropDuplicates()
        .cache()
    )

    qtd = dt_to_process.count()
    print(f"[RAW] Partições para processamento: {qtd}")

    if qtd == 0:
        print("[RAW] Nenhum backlog encontrado.")
        spark.stop()
        exit(0)

    # 🔥 Lazy load REAL
    df_inc = (
        spark.read
        .parquet(landing_path)
        .join(dt_to_process.hint("broadcast"), "dt")
    )

# =====================================================
# 2) Dedupe determinístico
# =====================================================
window_spec = Window.partitionBy(PRIMARY_KEY).orderBy(col(WATERMARK_COL).desc())

df_inc = (
    df_inc
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# =====================================================
# 3) Colunas técnicas
# =====================================================
df_inc = (
    df_inc
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source_system", lit("ecommerce_mysql"))
)

# =====================================================
# 4) Delta MERGE SAFE
# =====================================================
if not DeltaTable.isDeltaTable(spark, raw_path):

    print("[RAW] Primeira carga → criando Delta")

    (
        df_inc.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("dt")
        .save(raw_path)
    )

else:

    print("[RAW] Executando MERGE incremental (safe merge)")

    print("[RAW] Qtd registros antes do merge:",
          spark.read.format("delta").load(raw_path).count()
    )

    delta_table = DeltaTable.forPath(spark, raw_path)


    update_set = {c: f"source.{c}" for c in df_inc.columns}
    insert_values = {c: f"source.{c}" for c in df_inc.columns}

    (
        delta_table.alias("target")
        .merge(
            df_inc.alias("source"),
            f"target.{PRIMARY_KEY} = source.{PRIMARY_KEY}"
        )
        .whenMatchedUpdate(
            condition=f"source.{WATERMARK_COL} > target.{WATERMARK_COL}",
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

print("[RAW] MERGE concluído.")

print("[RAW] Qtd registros após o merge:",
      spark.read.format("delta").load(raw_path).count()
)

spark.stop()