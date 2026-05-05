# =====================================================
# IMPORTS
# =====================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

# =====================================================
# Configs
# =====================================================
TABLE = "clientes"
PRIMARY_KEY = "id_cliente"
WATERMARK_COL = "data_transacao"
LOOKBACK_DAYS = 0

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
# List Raw Partitions (Metadata Only)
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
# INCREMENTAL - UNPROCESSED + LOOKBACK
# =====================================================
else:

    print("[RAW] Incremental CDC-aware (Unprocessed + Lookback)")

    # -------------------------------------------------
    # Partitions already processed
    # -------------------------------------------------
    raw_dt_df = (
        spark.read
        .format("delta")
        .load(raw_path)
        .select("dt")
        .distinct()
    )

    # -------------------------------------------------
    # empty landing protection
    # -------------------------------------------------
    if landing_dt_df.count() == 0:
        print("[RAW] LANDING vazio")
        spark.stop()
        exit(0)

    # -------------------------------------------------
    # unprocessed
    # -------------------------------------------------
    unprocessed_dt_df = landing_dt_df.join(raw_dt_df, ["dt"], "left_anti")

    # -------------------------------------------------
    # DATA-BASED LOOKBACK
    # -------------------------------------------------
    lookback_df = (
        landing_dt_df
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

    print(f"[RAW] Partições para processamento: {qtd}")
    print("[RAW] Lista de partições:")
    for p in partitions:
        print(f" - {p}")

    if qtd == 0:
        print("[RAW] Nada para processar.")
        spark.stop()
        exit(0)

    # -------------------------------------------------
    # EFFICIENT READING (NO JOIN)
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
        F.col("id_cliente").cast("int").alias("id_cliente"),
        F.col("nome").cast("string").alias("nome"),
        F.col("cpf").cast("string").alias("cpf"),
        F.col("email").cast("string").alias("email"),
        F.col("telefone").cast("string").alias("telefone"),
        F.to_timestamp("data_cadastro").alias("data_cadastro"),
        F.to_timestamp("data_transacao").alias("data_transacao"),
        F.col("dt").cast("date").alias("dt")
    )
)

# ======================================================
# Data Label
# ======================================================
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