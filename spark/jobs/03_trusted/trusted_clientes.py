from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# =========================
# SPARK SESSION
# =========================

spark = (
    SparkSession.builder
    .appName("trusted_clientes")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

table = "clientes"
PRIMARY_KEY = "id_cliente"
WATERMARK_COL = "data_transacao"

raw_path = f"/data/02_raw/ecommerce/{table}"
trusted_path = f"/data/03_trusted/ecommerce/{table}"

# =========================
# READ RAW
# =========================

df = spark.read.format("delta").load(raw_path)

# =========================
# CLEANING
# =========================

df_clean = df.select(
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

# =========================
# DATA QUALITY
# =========================

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

# =========================
# CPF VALIDATION
# =========================

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

    # auditoria
    .withColumn(
        "processing_trusted",
        F.current_timestamp()
    )

    # remove colunas auxiliares
    .drop(
        "d1","d2","d3","d4","d5","d6",
        "d7","d8","d9","d10","d11",
        "dv1_calc","dv2_calc"
    )
)

# =========================
# WRITE TRUSTED
# =========================

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
        "[Trusted] Qtd registros antes do marge:",
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

print("[Trusted] Qtd registros Após o marge:",
    spark.read.format("delta").load(trusted_path).count()
)

spark.stop()