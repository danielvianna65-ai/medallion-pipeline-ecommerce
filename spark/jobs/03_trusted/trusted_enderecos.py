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
table = "enderecos"
PRIMARY_KEY = "id_endereco"
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
    .appName("trusted_enderecos")
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
normalized_customers_df= incremental_extract_df.select(
    "id_endereco",
    "id_cliente",

    # ---------------------
    # LOGRADOURO
    # ---------------------
    F.initcap(
        F.translate(
            F.regexp_replace(F.trim("logradouro"), r"\s+", " "),
            "áàãâéêíóôõúçÁÀÃÂÉÊÍÓÔÕÚÇ",
            "aaaaeeioooucAAAAEEIOOOUC"
        )
    ).alias("logradouro_clean"),

    # ---------------------
    # NUMERO
    # ---------------------
    F.upper(F.trim("numero")).alias("numero_clean"),

    # ---------------------
    # BAIRRO
    # ---------------------
    F.initcap(
        F.translate(
            F.regexp_replace(F.trim("bairro"), r"\s+", " "),
            "áàãâéêíóôõúçÁÀÃÂÉÊÍÓÔÕÚÇ",
            "aaaaeeioooucAAAAEEIOOOUC"
        )
    ).alias("bairro"),

    # ---------------------
    # CIDADE
    # ---------------------
    F.initcap(
        F.translate(
            F.regexp_replace(F.trim("cidade"), r"\s+", " "),
            "áàãâéêíóôõúçÁÀÃÂÉÊÍÓÔÕÚÇ",
            "aaaaeeioooucAAAAEEIOOOUC"
        )
    ).alias("cidade"),

    # ---------------------
    # ESTADO (UF)
    # ---------------------
    F.upper(
        F.regexp_replace(
            F.translate(
                F.trim("estado"),
                "áàãâéêíóôõúçÁÀÃÂÉÊÍÓÔÕÚÇ",
                "aaaaeeioooucAAAAEEIOOOUC"
            ),
            r"[^A-Za-z ]",
            ""
        )
    ).alias("estado_clean"),

    # ---------------------
    # CEP
    # ---------------------
    F.regexp_replace("cep", "[^0-9]", "").alias("cep"),

    "data_transacao",
    "dt",
    "source_system",
    "ingestion_ts"
)
# =====================================================
#  NORMALIZATION
# =====================================================

# ----------------------------
# ESTADO (UF)
# ----------------------------
mapping_estados = {
    "SAO PAULO": "SP",
    "SP": "SP",
    "RIO DE JANEIRO": "RJ",
    "RJ": "RJ",
    "PARANA": "PR",
    "PR": "PR",
    "MINAS GERAIS": "MG",
    "MG": "MG",
    "PERNAMBUCO": "PE",
    "PE": "PE",
}

mapping_expr = F.create_map([F.lit(x) for x in sum(mapping_estados.items(), ())])

standardized_customers_df = normalized_customers_df.withColumn(
    "estado",
    F.coalesce(
        mapping_expr[F.col("estado_clean")],
        F.col("estado_clean")
    )
).drop("estado_clean")

# ----------------------------
# LOGRADOURO
# ----------------------------
standardized_customers_df = standardized_customers_df.withColumn(
    "logradouro",
    F.regexp_replace(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.col("logradouro_clean"),
                    r"(?i)^av\.?\s", "Avenida "
                ),
                r"(?i)^r\.?\s", "Rua "
            ),
            r"(?i)^rod\.?\s", "Rodovia "
        ),
        r"(?i)^trav\.?\s", "Travessa "
    )
).drop("logradouro_clean")

# ---------------------
# NUMERO
# ---------------------
standardized_customers_df = standardized_customers_df.withColumn(
    "numero",
    F.when(
        F.col("numero_clean").rlike("^[0-9]+$"),
        F.col("numero_clean")
    ).otherwise(None)
).drop("numero_clean")


# ======================================================
# Data Quality Checks
# ======================================================
# ---------------------
# ESTADO (UF)
# ---------------------
ufs_validas = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO",
    "MA","MT","MS","MG","PA","PB","PR","PE","PI",
    "RJ","RN","RS","RO","RR","SC","SP","SE","TO"
]
validated_customers_df = standardized_customers_df.withColumn(
    "estado_valido",
    F.col("estado").isin(ufs_validas)
)

# ---------------------
# LOGRADOURO
# ---------------------
validated_customers_df = validated_customers_df.withColumn(
    "logradouro_valido",
    (
        (F.col("logradouro").isNotNull()) &
        (F.length("logradouro") > 3) &
        (~F.col("logradouro").rlike("^[0-9 ]+$"))
    )
)

# ---------------------
# NUMERO
# ---------------------
validated_customers_df = validated_customers_df.withColumn(
    "numero_valido",
    F.col("numero").isNotNull()
)

# ---------------------
# CEP
# ---------------------
validated_customers_df = (
    validated_customers_df
    .withColumn(
        "cep_valido",
        F.length("cep") == 8
    )
)

# ---------------------
# bairro
# ---------------------
validated_customers_df = validated_customers_df.withColumn(
    "bairro_valido",
    (
        (F.col("bairro").isNotNull()) &
        (F.length("bairro") > 3) &
        (~F.col("bairro").rlike("^[0-9 ]+$")) &
        (~F.col("bairro").isin("", "N/A", "NULL"))
    )
)

# ---------------------
# ADDRESS QUALITY
# ---------------------
validated_customers_df = validated_customers_df.withColumn(
    "qtd_validos_endereco",
    F.col("logradouro_valido").cast("int") +
    F.col("numero_valido").cast("int") +
    F.col("cep_valido").cast("int") +
    F.col("estado_valido").cast("int") +
    F.col("bairro_valido").cast("int")
)

# ======================================================
# Data Label
# ======================================================
labeled_customers_df = validated_customers_df.withColumn(
    "processing_trusted",
    F.current_timestamp()
)

# ======================================================
# Final Select
# ======================================================
trusted_customers_df = labeled_customers_df.select(
    "id_endereco",
    "id_cliente",
    "logradouro",
    "numero",
    "bairro",
    "cidade",
    "estado",
    "cep",
    "data_transacao",
    "dt",
    "source_system",
    "ingestion_ts",
    "logradouro_valido",
    "numero_valido",
    "bairro_valido",
    "estado_valido",
    "cep_valido",
    "qtd_validos_endereco",
    "processing_trusted"
)

# =====================================================
# Deterministic Dedupe
# =====================================================
customer_deduplication_window = Window.partitionBy(PRIMARY_KEY).orderBy(F.col(WATERMARK_COL).desc())

deduplicated_customers_df = (
    trusted_customers_df
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