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
table = "estoque"
PRIMARY_KEY = "id_estoque"
WATERMARK_COL = "data_transacao"
LOOKBACK_DAYS = 1

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
    .appName("trusted_estoque")
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

    incremental_extract_df = (
        spark.read
        .parquet(raw_path)
    )

# =====================================================
# INCREMENTAL - UNPROCESSED + LOOKBACK
# =====================================================
else:

    print("[trusted] Incremental (Unprocessed + Lookback)")

    # -------------------------------------------------
    # Partitions already processed
    # -------------------------------------------------
    processed_partitions_df = (
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
    unprocessed_dt_df  = raw_dt_df.join(processed_partitions_df, ["dt"], "left_anti")

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
    processing_partitions_df = (
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

    print(f"[trusted] Partições para processamento: {processing_partition_count}")
    print("[trusted] Lista de partições:")
    for p in sorted_processing_partitions:
        print(f" - {p}")

    if processing_partition_count == 0:
        print("[trusted] Nada para processar.")
        spark.stop()
        exit(0)

    # -------------------------------------------------
    # EFFICIENT READING (NO JOIN)
    # -------------------------------------------------
    incremental_extract_df = (
        spark.read
        .parquet(raw_path)
        .filter(F.col("dt").isin(processing_partition_list))
    )

# ======================================================
# Data Quality
# ======================================================
validated_customers_df = (
    incremental_extract_df
    .withColumn(
        "quantidade_valida",
        F.col("quantidade_disponivel") >= 0
    )
)

# ======================================================
# Data Label
# ======================================================
labeled_customers_df = validated_customers_df.withColumn(
    "processing_trusted",
    F.current_timestamp()
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
if not DeltaTable.isDeltaTable(spark, trusted_path):

    print(f"[Trusted][{table}] Bootstrap inicial")

    (
        deduplicated_customers_df.write
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

    trusted_customers_delta_table = DeltaTable.forPath(spark, trusted_path)

    update_set = {c: f"source.{c}" for c in deduplicated_customers_df.columns}
    insert_set = {c: f"source.{c}" for c in deduplicated_customers_df.columns}

    (
        trusted_customers_delta_table.alias("target")
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

print(f"[Trusted][{table}] MERGE concluído")

print(
    f"[Trusted][{table}] Qtd registros após o merge:",
    spark.read.format("delta").load(trusted_path).count()
)

spark.stop()
