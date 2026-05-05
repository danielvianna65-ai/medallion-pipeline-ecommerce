# =====================================================
# IMPORTS
# =====================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# =====================================================
# Configs
# =====================================================
table = "produtos"
PRIMARY_KEY = "id_produto"
WATERMARK_COL = "data_transacao"
LOOKBACK_DAYS = 0

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
    .appName("trusted_produtos")
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
# List Raw Partitions (Metadata Only)
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
# INCREMENTAL - UNPROCESSED + LOOKBACK
# =====================================================
else:

    print("[trusted] Incremental CDC-aware (Unprocessed + Lookback)")

    # -------------------------------------------------
    # Partitions already processed
    # -------------------------------------------------
    trusted_dt_df = (
        spark.read
        .format("delta")
        .load(trusted_path)
        .select("dt")
        .distinct()
    )

    # -------------------------------------------------
    # Empty RAW protection
    # -------------------------------------------------
    if raw_dt_df.count() == 0:
        print("[trusted] RAW vazio")
        spark.stop()
        exit(0)

    # -------------------------------------------------
    # unprocessed
    # -------------------------------------------------
    unprocessed_dt_df  = raw_dt_df.join(trusted_dt_df, ["dt"], "left_anti")

    # -------------------------------------------------
    # DATA-BASED LOOKBACK
    # -------------------------------------------------
    lookback_df = (
        raw_dt_df
        .select("dt")
        .distinct()
        .orderBy(F.col("dt").desc())
        .limit(LOOKBACK_DAYS)
    )

    # -------------------------------------------------
    # UNION FINAL OF PARTITIONS
    # -------------------------------------------------
    dt_valid = (
        unprocessed_dt_df
        .union(lookback_df)
        .dropDuplicates()
    )

    # -------------------------------------------------
    # single collection
    # -------------------------------------------------
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

    # -------------------------------------------------
    # EFFICIENT READING (NO JOIN)
    # -------------------------------------------------
    df_inc = (
        spark.read
        .parquet(raw_path)
        .filter(F.col("dt").isin(dt_list))
    )

    # -------------------------------------------------
    # single collection
    # -------------------------------------------------
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

    # -------------------------------------------------
    # EFFICIENT READING (NO JOIN)
    # -------------------------------------------------
    df_inc = (
        spark.read
        .parquet(raw_path)
        .filter(F.col("dt").isin(dt_list))
    )

# =====================================================
# CLEANING
# =====================================================
df_clean = df_inc.select(

    "id_produto",
    "id_categoria",

    # nome padronizado
    F.initcap(
        F.regexp_replace(
            F.trim("nome_produto"),
            "\\s+",
            " "
        )
    ).alias("nome_produto"),

    # descricao
    F.trim("descricao").alias("descricao"),
    "preco",
    "ativo",
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
    .withColumn("preco_valido", F.col("preco") >= 0)
    .withColumn(
        "ativo_valido",
        F.when(F.col("ativo").isNull(), False)
        .when(F.col("ativo").isin(True, False), True)
        .otherwise(False)
    )
)

# =====================================================
# BUSINESS RULE
# =====================================================
window_spec = Window.partitionBy("nome_produto").orderBy(
    F.col("ativo").desc(),
    F.col("data_transacao").desc()
)

df_clean = (
    df_clean
    .withColumn("rn", F.row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)

# ======================================================
# Data Label
# ======================================================
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

    print(f"[Trusted][{table}] Bootstrap inicial")

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("dt")
        .save(trusted_path)
    )

else:

    print(f"[Trusted][{table}] Executando MERGE incremental")

    print(
        f"[Trusted][{table}] Qtd registros antes do merge:",
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

print(f"[Trusted][{table}] MERGE concluído")

print(
    f"[Trusted][{table}] Qtd registros após o merge:",
    spark.read.format("delta").load(trusted_path).count()
)

spark.stop()