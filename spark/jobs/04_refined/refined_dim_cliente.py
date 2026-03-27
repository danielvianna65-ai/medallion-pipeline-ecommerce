from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("dim_cliente")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

base = "/data/03_trusted/ecommerce"
refined = "/data/04_refined/ecommerce/dim_cliente"

# ======================================================
# READ
# ======================================================

dfc = spark.read.format("delta").load(f"{base}/clientes")
dfe = spark.read.format("delta").load(f"{base}/clientes_enrichment")

# ======================================================
# PADRONIZAÇÃO + VALIDAÇÃO CPF
# ======================================================
dfc = dfc.withColumn(
    "cpf",
    F.when(F.length("cpf") == 11, F.col("cpf"))
     .otherwise(None)
)

dfe = dfe.withColumn(
    "cpf",
    F.when(F.length("cpf") == 11, F.col("cpf"))
     .otherwise(None)
)


dfc = dfc.filter(F.col("cpf").isNotNull())
dfe = dfe.filter(F.col("cpf").isNotNull())

# ======================================================
# JOIN CONTROLADO
# ======================================================

df = (
    dfc.alias("c")
    .join(dfe.alias("e"), "cpf", "left")
    .select(
        F.col("c.id_cliente"),
        F.col("c.cpf"),

        F.coalesce(F.col("e.nome"), F.col("c.nome")).alias("nome"),
        F.coalesce(F.col("e.email"), F.col("c.email")).alias("email"),
        F.coalesce(F.col("e.telefone"), F.col("c.telefone")).alias("telefone"),

        # 🔹 enrichment puro (não existe no c)
        F.col("e.renda_estimada").alias("renda_estimada"),
        F.col("e.score_credito").alias("score_credito"),

        F.col("c.ingestion_ts") # dedup

    )
)

# ======================================================
# DEDUP (ÚLTIMO REGISTRO)
# ======================================================

window = Window.partitionBy("cpf").orderBy(F.col("ingestion_ts").desc())

df = (
    df.withColumn("rn", F.row_number().over(window))
      .filter("rn = 1")
      .drop("rn")
)

df = df.drop("ingestion_ts")

# ======================================================
# COLUNAS SCD2
# ======================================================

df = df.withColumn("dt_inicio", F.current_timestamp())
df = df.withColumn("dt_fim", F.lit(None).cast("timestamp"))
df = df.withColumn("is_current", F.lit(True))

# ======================================================
# SK + HASH
# ======================================================

df = df.withColumn(
    "sk_cliente",
    F.abs(F.hash("cpf", "dt_inicio")).cast("bigint")
)

df = df.withColumn(
    "hash_diff",
    F.sha2(
        F.concat_ws("||",
            "nome",
            "email",
            "telefone",
            "renda_estimada",
            "score_credito"
        ),
        256
    )
)

# ======================================================
# WRITE (SCD2 MERGE)
# ======================================================

if not DeltaTable.isDeltaTable(spark, refined):

    df.write.format("delta").mode("overwrite").save(refined)

else:

    delta = DeltaTable.forPath(spark, refined)

    # =========================================
    # 1. EXPIRAR
    # =========================================
    (
        delta.alias("t")
        .merge(
            df.alias("s"),
            "t.cpf = s.cpf AND t.is_current = true"
        )
        .whenMatchedUpdate(
            condition="t.hash_diff <> s.hash_diff",
            set={
                "dt_fim": "current_timestamp()",
                "is_current": "false"
            }
        )
        .execute()
    )

    # =========================================
    # 2. IDENTIFICAR NOVOS
    # =========================================
    df_new = (
        df.alias("s")
        .join(
            delta.toDF().alias("t"),
            (F.col("s.cpf") == F.col("t.cpf")) & (F.col("t.is_current") == True),
            "left"
        )
        .where(
            (F.col("t.cpf").isNull()) |
            (F.col("t.hash_diff") != F.col("s.hash_diff"))
        )
        .select("s.*")
    )

    # =========================================
    # 3. INSERIR
    # =========================================
    (
        delta.alias("t")
        .merge(
            df_new.alias("s"),
            "t.cpf = s.cpf AND t.is_current = true"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

print("[DIM_CLIENTE] OK")

spark.stop()