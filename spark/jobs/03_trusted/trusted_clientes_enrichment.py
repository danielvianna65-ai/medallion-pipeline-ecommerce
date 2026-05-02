# ======================================================
# IMPORTS
# ======================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# =====================================================
# Config
# =====================================================
table = "clientes_enrichment"

# ======================================================
# PATHS
# ======================================================
raw_path = f"/data/02_raw/ecommerce/{table}"
trusted_path = f"/data/03_trusted/ecommerce/{table}"
clientes_path = "/data/03_trusted/ecommerce/clientes"

# =====================================================
# Spark Session Delta
# =====================================================
spark = (
    SparkSession.builder
    .appName("trusted_clientes_enrichmentcount5")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ======================================================
# READ RAW
# ======================================================
print(f"[TRUSTED][{table}] Source RAW: {raw_path}")
print(f"[TRUSTED][{table}] Lookup CLIENTES (TRUSTED): {clientes_path}")
print(f"[TRUSTED][{table}] Target TRUSTED: {trusted_path}")

df_raw = spark.read.format("delta").load(raw_path)

# ======================================================
# DATA QUALITY + NORMALIZATION
# ======================================================
print(f"[TRUSTED][{table}] Aplicando limpeza e padronização")

df_clean = (
    df_raw
    # CPF
    .withColumn("cpf", F.regexp_replace(F.col("cpf"), "[^0-9]", ""))

    # Nome
    .withColumn(
        "nome",
        F.translate(
            F.regexp_replace(
                F.initcap(
                    F.regexp_replace(
                        F.regexp_replace(F.trim(F.col("nome")), "_", " "),
                        "[^\\p{L} ]",
                        ""
                    )
                ),
                "\\s+",
                " "
            ),
            "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
            "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"
        )
    )

    # Telefone (STRING + normalizado)
    .withColumn("telefone", F.regexp_replace(F.col("telefone"), "[^0-9]", ""))

    # Email
    .withColumn("email", F.lower(F.trim(F.col("email"))))

    # CPF válido
    .filter(F.col("cpf").rlike("^[0-9]{11}$"))
)

# ======================================================
# Data Quality Checks
# ======================================================
df_clean = (
    df_clean

    # Email válido
    .withColumn(
        "email",
        F.when(
            F.col("email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$"),
            F.col("email")
        )
    )

    # Telefone válido (10 ou 11 dígitos)
    .withColumn(
        "telefone",
        F.when(
            F.col("telefone").rlike("^[0-9]{10,11}$"),
            F.col("telefone")
        )
    )
)

# ======================================================
# Deterministic Deduplication
# ======================================================
print(f"[TRUSTED][{table}] Deduplicando enrichment")

w = Window.partitionBy("cpf").orderBy(F.col("ingestion_ts").desc())

df_clean = (
    df_clean
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)

# ======================================================
# Filter Existing CPFs in (clientes_trusted)
# ======================================================
print("[TRUSTED] Validando domínio (clientes)")

df_clientes = (
    spark.read
    .format("delta")
    .load(clientes_path)
    .select("cpf")
    .dropDuplicates()
)

df_match = (
    df_clean.alias("e")
    .join(
        F.broadcast(df_clientes.alias("c")),
        F.col("e.cpf") == F.col("c.cpf"),
        "inner"
    )
    .select("e.*")
)

print(f"[TRUSTED][{table}] Registros válidos: {df_match.count()}")

# ======================================================
# Data Auditing
# ======================================================
df_match = df_match.withColumn(
    "processing_trusted",
    F.current_timestamp()
)

# ======================================================
# Incremental Merge
# ======================================================
is_bootstrap = not DeltaTable.isDeltaTable(spark, trusted_path)

if is_bootstrap:

    print(f"[TRUSTED][{table}] Bootstrap inicial")

    (
        df_match.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(trusted_path)
    )

else:

    print(f"[TRUSTED][{table}] Executando MERGE incremental")

    delta_table = DeltaTable.forPath(spark, trusted_path)

    update_set = {
        "nome": "source.nome",
        "email": "source.email",
        "telefone": "source.telefone",
        "renda_estimada": "source.renda_estimada",
        "score_credito": "source.score_credito",
        "ingestion_ts": "source.ingestion_ts",
        "source_system": "source.source_system",
        "processing_trusted": "source.processing_trusted"
    }

    insert_set = {
        "cpf": "source.cpf",
        "nome": "source.nome",
        "email": "source.email",
        "telefone": "source.telefone",
        "renda_estimada": "source.renda_estimada",
        "score_credito": "source.score_credito",
        "ingestion_ts": "source.ingestion_ts",
        "source_system": "source.source_system",
        "processing_trusted": "source.processing_trusted"
    }

    (
        delta_table.alias("target")
        .merge(
            df_match.alias("source"),
            "target.cpf = source.cpf"
        )
        .whenMatchedUpdate(
            condition="""
                source.ingestion_ts >= target.ingestion_ts AND (
                    COALESCE(target.nome, '') <> COALESCE(source.nome, '') OR
                    COALESCE(target.email, '') <> COALESCE(source.email, '') OR
                    COALESCE(target.telefone, '') <> COALESCE(source.telefone, '') OR
                    COALESCE(target.renda_estimada, 0) <> COALESCE(source.renda_estimada, 0) OR
                    COALESCE(target.score_credito, -1) <> COALESCE(source.score_credito, -1)
                )
            """,
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_set)
        .execute()
    )

print(f"[TRUSTED][{table}] Pipeline concluído com sucesso")

spark.stop()