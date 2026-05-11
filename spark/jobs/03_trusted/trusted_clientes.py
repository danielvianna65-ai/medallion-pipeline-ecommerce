# =====================================================
# IMPORTS
# =====================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# =====================================================
# Configs
# =====================================================
table = "clientes"
PRIMARY_KEY = "id_cliente"
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
    .appName("trusted_clientes")
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

# =====================================================
# CLEANING
# =====================================================
normalized_customers_df = incremental_extract_df.select(
    "id_cliente",

    # ---------------------
    # NOME
    # ---------------------
    F.translate(
        F.regexp_replace(
            F.initcap(
                F.regexp_replace(
                    F.regexp_replace(
                        F.trim("nome"),
                        "_",  # troca underscore por espaço
                        " "
                    ),
                    "[^\\p{L} ]",  # remove caracteres inválidos
                    ""
                )
            ),
            "\\s+",
            " "
        ),
        "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
        "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"
    ).alias("nome"),

    # ---------------------
    # CPF
    # ---------------------
    F.regexp_replace("cpf", "[^0-9]", "").alias("cpf"),

    # ---------------------
    # EMAIL
    # ---------------------
    F.lower(F.trim("email")).alias("email"),

    # ---------------------
    # TELEFONE
    # ---------------------
    F.regexp_replace("telefone", "[^0-9]", "").alias("telefone"),

    "data_transacao",
    "dt",
    "source_system",
    "ingestion_ts"
)

# =====================================================
# DATA QUALITY
# =====================================================
validated_customers_df = (
    normalized_customers_df

    # email valido
    .withColumn(
        "email_valido",
        F.col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")
    )

    # telefone valido
    .withColumn(
        "telefone_valido",
        F.length("telefone").between(10,11)
    )
)

# =====================================================
# CPF VALIDATION
# =====================================================
cpf_validated_customers_df = validated_customers_df

for i in range(1, 12):
    cpf_validated_customers_df = (
        cpf_validated_customers_df.withColumn(
            f"d{i}",
            F.substring("cpf", i, 1).cast("int")
        )
    )

cpf_validated_customers_df = (
    cpf_validated_customers_df

    # dv1
    .withColumn(
        "dv1_calc",
        (
            F.col("d1")*10 + F.col("d2")*9 + F.col("d3")*8 +
            F.col("d4")*7 + F.col("d5")*6 + F.col("d6")*5 +
            F.col("d7")*4 + F.col("d8")*3 + F.col("d9")*2
        ) % 11
    )

    .withColumn(
        "dv1_calc",
        F.when(F.col("dv1_calc") < 2, 0)
        .otherwise(11 - F.col("dv1_calc"))
    )

    # dv2
    .withColumn(
        "dv2_calc",
        (
            F.col("d1")*11 + F.col("d2")*10 + F.col("d3")*9 +
            F.col("d4")*8 + F.col("d5")*7 + F.col("d6")*6 +
            F.col("d7")*5 + F.col("d8")*4 + F.col("d9")*3 +
            F.col("dv1_calc")*2
        ) % 11
    )

    .withColumn(
        "dv2_calc",
        F.when(F.col("dv2_calc") < 2, 0)
        .otherwise(11 - F.col("dv2_calc"))
    )

    # validação final
    .withColumn(
        "cpf_valido",
        (
            (F.col("dv1_calc") == F.col("d10")) &
            (F.col("dv2_calc") == F.col("d11")) &
            (F.length("cpf") == 11) &
            (~F.col("cpf").rlike(r"^(\d)\1{10}$"))
        )
    )

    # remove colunas auxiliares
    .drop(
        "d1","d2","d3","d4","d5","d6",
        "d7","d8","d9","d10","d11",
        "dv1_calc","dv2_calc"
    )
)

# ======================================================
# Data Label
# ======================================================
labeled_customers_df = cpf_validated_customers_df.withColumn(
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
            f"target.{PRIMARY_KEY} = source.{PRIMARY_KEY} AND target.dt = source.dt"
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