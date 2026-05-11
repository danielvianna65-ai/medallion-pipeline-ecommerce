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
TABLE = "estoque"
PRIMARY_KEY = "id_estoque"
WATERMARK_COL = "data_transacao"
LOOKBACK_DAYS = 1

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

    incremental_extract_df = (
        spark.read
        .parquet(landing_path)
    )

# =====================================================
# INCREMENTAL - UNPROCESSED + LOOKBACK
# =====================================================
else:

    print("[RAW] Incremental (Unprocessed + Lookback)")

    # -------------------------------------------------
    # Partitions already processed
    # -------------------------------------------------
    processed_partitions_df = (
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
    unprocessed_dt_df = landing_dt_df.join(processed_partitions_df, ["dt"], "left_anti")

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
    processing_partitions_df= (
        unprocessed_dt_df
        .union(lookback_df)
        .dropDuplicates()
    )

    # -------------------------------------------------
    # single collection
    # -------------------------------------------------
    processing_partition_rows = processing_partitions_df.collect()

    processing_partition_count = len(processing_partition_rows)
    processing_partition_list = [r.dt for r in processing_partition_rows]
    sorted_processing_partitions = sorted(processing_partition_list)

    print(f"[RAW] Partições para processamento: {processing_partition_count}")
    print("[RAW] Lista de partições:")
    for p in sorted_processing_partitions:
        print(f" - {p}")

    if processing_partition_count == 0:
        print("[RAW] Nada para processar.")
        spark.stop()
        exit(0)

    # -------------------------------------------------
    # EFFICIENT READING (NO JOIN)
    # -------------------------------------------------
    incremental_extract_df = (
        spark.read
        .parquet(landing_path)
        .filter(F.col("dt").isin(processing_partition_list))
    )

# =====================================================
# SCHEMA
# =====================================================
standardized_customers_df = (
    incremental_extract_df.select(
        F.col("id_estoque").cast("int").alias("id_estoque"),
        F.col("id_produto").cast("int").alias("id_produto"),
        F.col("quantidade_disponivel").cast("int").alias("quantidade_disponivel"),
        F.to_timestamp("data_transacao").alias("data_transacao"),
        F.col("dt").cast("date").alias("dt")
    )
)

# ======================================================
# Data Label
# ======================================================
labeled_customers_df = (
    standardized_customers_df
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("source_system", F.lit("ecommerce_mysql"))
)

# =====================================================
# Deterministic Dedupe
# =====================================================
customer_deduplication_window = Window.partitionBy(PRIMARY_KEY).orderBy(F.col(WATERMARK_COL).desc())

deduplicated_customers_df = (
    labeled_customers_df
    .withColumn("rn", F.row_number().over(customer_deduplication_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ======================================================
# Incremental Merge
# ======================================================
if not DeltaTable.isDeltaTable(spark, raw_path):

    print("[RAW] Primeira carga → criando Delta")

    (
        deduplicated_customers_df.write
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

    update_set = {c: f"source.{c}" for c in deduplicated_customers_df.columns}
    insert_set = {c: f"source.{c}" for c in deduplicated_customers_df.columns}

    (
        delta_table.alias("target")
        .merge(
            deduplicated_customers_df.alias("source"),
            f"target.{PRIMARY_KEY} = source.{PRIMARY_KEY}"
        )
        .whenMatchedUpdate(
            condition=f"source.{WATERMARK_COL} > target.{WATERMARK_COL}",
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_set)
        .execute()
    )

print("[RAW] MERGE concluído.")

print("[RAW] Qtd registros após o merge:",
      spark.read.format("delta").load(raw_path).count()
)

spark.stop()