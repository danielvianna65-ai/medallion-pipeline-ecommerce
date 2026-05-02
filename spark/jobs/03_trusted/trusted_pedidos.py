# =====================================================
# IMPORTS
# =====================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from datetime import datetime, timedelta
from pyspark.sql.window import Window

# =====================================================
# Configs
# =====================================================
table = "pedidos"
PRIMARY_KEY = "id_pedido"
WATERMARK_COL = "data_transacao"
LOOKBACK_DAYS = 2

# =====================================================
# Paths
# =====================================================
raw_path = f"/data/02_raw/ecommerce/{table}"
trusted_path = f"/data/03_trusted/ecommerce/{table}"

# =====================================================
# Spark Session Delta
# =====================================================
spark = (
    SparkSession.builder
    .appName("trusted_pedidos")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
print(f"[TRUSTED][{table}] Source: {raw_path}")
print(f"[TRUSTED][{table}] Target: {trusted_path}")

# =====================================================
# CHECK BOOTSTRAP
# =====================================================
is_bootstrap = not DeltaTable.isDeltaTable(spark, trusted_path)

# =====================================================
# LISTAR PARTIÇÕES RAW (METADATA ONLY)
# =====================================================
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

raw_path_j = spark._jvm.org.apache.hadoop.fs.Path(raw_path)
status = fs.listStatus(raw_path_j)

raw_dt_list = [
    s.getPath().getName().replace("dt=", "")
    for s in status
    if s.getPath().getName().startswith("dt=")
]

raw_dt_df = spark.createDataFrame(
    [(d,) for d in raw_dt_list], ["dt"]
).withColumn("dt", F.to_date("dt"))

# =====================================================
# BOOTSTRAP
# =====================================================
if is_bootstrap:

    print("[trusted] Bootstrap FULL")

    df_inc = (
        spark.read
        .parquet(raw_path)
    )

# =====================================================
# INCREMENTAL + BACKLOG + LOOKBACK
# =====================================================
else:

    print("[trusted] Incremental CDC-aware (backlog + lookback)")

    # -------------------------------------------------
    # PARTIÇÕES JÁ PROCESSADAS
    # -------------------------------------------------
    trusted_dt_df = (
        spark.read
        .format("delta")
        .load(trusted_path)
        .select("dt")
        .distinct()
    )

    # -------------------------------------------------
    # proteção RAW vazio
    # -------------------------------------------------
    if raw_dt_df.count() == 0:
        print("[trusted] RAW vazio")
        spark.stop()
        exit(0)

    # -------------------------------------------------
    # BACKLOG (NOVOS DIAS)
    # -------------------------------------------------
    backlog_dt = raw_dt_df.join(trusted_dt_df, ["dt"], "left_anti")

    # -------------------------------------------------
    # LOOKBACK BASEADO NO DADO
    # -------------------------------------------------
    max_dt = raw_dt_df.agg(F.max("dt")).collect()[0][0]

    recent_days = [
        (max_dt - timedelta(days=i)).isoformat()
        for i in range(LOOKBACK_DAYS)
    ]

    recent_df = spark.createDataFrame([(d,) for d in recent_days], ["dt"]) \
        .withColumn("dt", F.to_date("dt")) \
        .intersect(raw_dt_df)

    # -------------------------------------------------
    # UNION FINAL DE PARTIÇÕES
    # -------------------------------------------------
    dt_valid = (
        backlog_dt
        .union(recent_df)
        .dropDuplicates()
    )

    dt_rows = dt_valid.collect()

    qtd = len(dt_rows)
    dt_list = [r.dt for r in dt_rows]
    partitions = sorted(dt_list)

    print(f"[trusted] Partições para processamento: {qtd}")
    print("[trusted] Lista de partições:")
    for p in partitions:
        print(f" - {p}")

    if qtd == 0:
        print("[trusted] Nada para processar.")
        spark.stop()
        exit(0)

    # leitura
    df_inc = (
        spark.read
        .parquet(raw_path)
        .filter(F.col("dt").isin(dt_list))
    )

# =====================================================
# CLEANING
# =====================================================
df_clean = df_inc.select(

    "id_pedido",
    "id_cliente",
    "id_endereco",
    "data_pedido",
    F.lower(F.trim("status_pedido")).alias("status_pedido"),
    "valor_total",
    "data_transacao",
    "dt",
    "source_system",
    "ingestion_ts"
)

# =====================================================
# DATA QUALITY
# =====================================================
df_clean = (
    df_clean

    # valor válido
    .withColumn(
        "valor_valido",
        F.col("valor_total") >= 0
    )

    # status válido
    .withColumn(
        "status_valido",
        F.col("status_pedido").isin(
            "em_processamento", "pago", "cancelado"
        )
    )

    # data_pedido válida
    .withColumn(
        "data_pedido_valida",
        F.col("data_pedido").isNotNull()
    )
)

# =====================================================
# Data Auditing
# =====================================================
df_clean = df_clean.withColumn(
    "processing_trusted",
    F.current_timestamp()
)

# =====================================================
# Dedupe determinístic
# =====================================================
window_spec = Window.partitionBy(PRIMARY_KEY).orderBy(F.col(WATERMARK_COL).desc())

df_clean = (
    df_clean
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ======================================================
# Incremental Merge
# ======================================================
if not DeltaTable.isDeltaTable(spark, trusted_path):

    print("[Trusted] Bootstrap inicial")

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("dt")
        .save(trusted_path)
    )

else:

    print("[Trusted] Executando MERGE incremental")

    print(
        "[Trusted] Qtd registros antes do merge:",
        spark.read.format("delta").load(trusted_path).count()
    )

    delta_table = DeltaTable.forPath(spark, trusted_path)

    update_set = {c: f"source.{c}" for c in df_clean.columns}
    insert_values = {c: f"source.{c}" for c in df_clean.columns}

    (
        delta_table.alias("target")
        .merge(
            df_clean.alias("source"),
            f"target.{PRIMARY_KEY} = source.{PRIMARY_KEY}"
        )
        .whenMatchedUpdate(
            condition=f"source.{WATERMARK_COL} > target.{WATERMARK_COL}",
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

print("[Trusted] MERGE concluído")

print(
    "[Trusted] Qtd registros após o merge:",
    spark.read.format("delta").load(trusted_path).count()
)

spark.stop()