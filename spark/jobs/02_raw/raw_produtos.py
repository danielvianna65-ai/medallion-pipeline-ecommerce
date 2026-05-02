# =====================================================
# IMPORTS
# =====================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# =====================================================
# Configs
# =====================================================
TABLE = "produtos"
PRIMARY_KEY = "id_produto"
WATERMARK_COL = "data_transacao"
LOOKBACK_DAYS = 2

# =====================================================
# Paths
# =====================================================
landing_path = f"/data/01_landing/ecommerce/{TABLE}"
raw_path = f"/data/02_raw/ecommerce/{TABLE}"

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
print(f"[RAW][{TABLE}] Source: {landing_path}")
print(f"[RAW][{TABLE}] Target: {raw_path}")

# =====================================================
# CHECK BOOTSTRAP
# =====================================================
is_bootstrap = not DeltaTable.isDeltaTable(spark, raw_path)

# =====================================================
# LISTAR PARTIÇÕES LANDING (METADATA ONLY)
# =====================================================
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

landing_path_j = spark._jvm.org.apache.hadoop.fs.Path(landing_path)
status = fs.listStatus(landing_path_j)

landing_dt_list = [
    s.getPath().getName().replace("dt=", "")
    for s in status
    if s.getPath().getName().startswith("dt=")
]

landing_dt_df = spark.createDataFrame(
    [(d,) for d in landing_dt_list], ["dt"]
).withColumn("dt", F.to_date("dt"))

# =====================================================
# BOOTSTRAP
# =====================================================
if is_bootstrap:

    print("[RAW] Bootstrap FULL")

    df_inc = (
        spark.read
        .parquet(landing_path)
    )

# =====================================================
# INCREMENTAL + BACKLOG + LOOKBACK
# =====================================================
else:

    print("[RAW] Incremental CDC-aware (backlog + lookback)")

    # -------------------------------------------------
    # PARTIÇÕES JÁ PROCESSADAS
    # -------------------------------------------------
    raw_dt_df = (
        spark.read
        .format("delta")
        .load(raw_path)
        .select("dt")
        .distinct()
    )

    # -------------------------------------------------
    # proteção LANDING vazio
    # -------------------------------------------------
    if landing_dt_df.count() == 0:
        print("[RAW] LANDING vazio")
        spark.stop()
        exit(0)

    # -------------------------------------------------
    # BACKLOG (NOVOS DIAS)
    # -------------------------------------------------
    backlog_dt = landing_dt_df.join(raw_dt_df, ["dt"], "left_anti")

    # -------------------------------------------------
    # LOOKBACK BASEADO NO DADO (não no relógio)
    # -------------------------------------------------
    max_dt = landing_dt_df.agg(F.max("dt")).collect()[0][0]

    recent_days = [
        (max_dt - timedelta(days=i)).isoformat()
        for i in range(LOOKBACK_DAYS)
    ]

    recent_df = spark.createDataFrame([(d,) for d in recent_days], ["dt"]) \
        .withColumn("dt", F.to_date("dt")) \
        .intersect(landing_dt_df)

    # -------------------------------------------------
    # UNION FINAL DE PARTIÇÕES
    # -------------------------------------------------
    dt_valid = (
        backlog_dt
        .union(recent_df)
        .dropDuplicates()
    )

    # -------------------------------------------------
    # coleta única
    # -------------------------------------------------
    dt_rows = dt_valid.collect()

    qtd = len(dt_rows)
    dt_list = [r.dt for r in dt_rows]
    partitions = sorted(dt_list)

    print(f"[RAW] Partições para processamento: {qtd}")
    print("[RAW] Lista de partições:")
    for p in partitions:
        print(f" - {p}")

    if qtd == 0:
        print("[RAW] Nada para processar.")
        spark.stop()
        exit(0)

    # -------------------------------------------------
    # LEITURA EFICIENTE (SEM JOIN)
    # -------------------------------------------------
    df_inc = (
        spark.read
        .parquet(landing_path)
        .filter(F.col("dt").isin(dt_list))
    )

# =====================================================
# SCHEMA
# =====================================================
df_inc = (
    df_inc.select(
        F.col("id_produto").cast("int").alias("id_produto"),
        F.col("id_categoria").cast("int").alias("id_categoria"),
        F.col("nome_produto").cast("string").alias("nome_produto"),
        F.col("descricao").cast("string").alias("descricao"),
        F.col("preco").cast(DecimalType(12, 2)).alias("preco"),
        F.col("ativo").cast("boolean").alias("ativo"),
        F.to_timestamp("data_transacao").alias("data_transacao"),
        F.col("dt").cast("date").alias("dt")
    )
)

# =====================================================
# Colunas técnicas
# =====================================================
df_inc = (
    df_inc
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("source_system", F.lit("ecommerce_mysql"))
)

# =====================================================
# Dedupe determinístic
# =====================================================
window_spec = Window.partitionBy(PRIMARY_KEY).orderBy(F.col(WATERMARK_COL).desc())

df_inc = (
    df_inc
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# =====================================================
# Delta MERGE SAFE
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
          spark.read.format("delta").load(raw_path).count())

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