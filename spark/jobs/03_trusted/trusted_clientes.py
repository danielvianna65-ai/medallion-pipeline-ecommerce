# =====================================================
# IMPORTS
# =====================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from datetime import  timedelta
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
df_clean = (
    df_clean

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
for i in range(1,12):
    df_clean = df_clean.withColumn(
        f"d{i}",
        F.substring("cpf",i,1).cast("int")
    )

df_clean = (
    df_clean

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
        f"[Trusted][{table}] Qtd registros antes do marge:",
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

print(f"[Trusted][{table}] Qtd registros Após o marge:",
    spark.read.format("delta").load(trusted_path).count()
)

spark.stop()