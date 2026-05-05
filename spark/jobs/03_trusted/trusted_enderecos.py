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

# =====================================================
# CLEANING
# =====================================================
df_clean = df_inc.select(
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

df_clean = df_clean.withColumn(
    "estado",
    F.coalesce(
        mapping_expr[F.col("estado_clean")],
        F.col("estado_clean")
    )
).drop("estado_clean")

# ----------------------------
# LOGRADOURO
# ----------------------------
df_clean = df_clean.withColumn(
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
df_clean = df_clean.withColumn(
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
df_clean = df_clean.withColumn(
    "estado_valido",
    F.col("estado").isin(ufs_validas)
)

# ---------------------
# LOGRADOURO
# ---------------------
df_clean = df_clean.withColumn(
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
df_clean = df_clean.withColumn(
    "numero_valido",
    F.col("numero").isNotNull()
)

# ---------------------
# CEP
# ---------------------
df_clean = (
    df_clean
    .withColumn(
        "cep_valido",
        F.length("cep") == 8
    )
)

# ---------------------
# bairro
# ---------------------
df_clean = df_clean.withColumn(
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
df_clean = df_clean.withColumn(
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
df_clean = df_clean.withColumn(
    "processing_trusted",
    F.current_timestamp()
)

# ======================================================
# Final Select
# ======================================================
df_clean = df_clean.select(
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

    print(f"[Trusted][{table}]  Bootstrap inicial")

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("dt")
        .save(trusted_path)
    )

else:

    print(f"[Trusted][{table}]  Executando MERGE incremental")

    print(
        f"[Trusted][{table}]  Qtd registros antes do merge:",
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

print(f"[Trusted][{table}]  MERGE concluído")

print(
    f"[Trusted][{table}]  Qtd registros após o merge:",
    spark.read.format("delta").load(trusted_path).count()
)

spark.stop()