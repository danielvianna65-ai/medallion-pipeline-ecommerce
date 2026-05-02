# ======================================================
# IMPORTS
# ======================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# =====================================================
# Paths
# =====================================================
prod = "/data/03_trusted/ecommerce/produtos"
cat = "/data/03_trusted/ecommerce/categorias"
refined = "/data/04_refined/ecommerce/dim_produto"

# =====================================================
# Spark Session Delta
# =====================================================
spark = (
    SparkSession.builder
    .appName("dim_produto_scd2")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# =====================================================
# READ TRUSTED/REFINED
# =====================================================
df_prod = spark.read.format("delta").load(prod)
dim_cat = spark.read.format("delta").load(cat)

# =====================================================
# JOIN
# =====================================================
df = (
    df_prod.alias("p")
    .join(
        dim_cat.alias("c"),
        F.col("p.id_categoria") == F.col("c.id_categoria"),
        "left"
    )
)

# =====================================================
# SELECT / JOIN
# =====================================================
df = df.select(
    "p.id_produto",
    "p.nome_produto",
    "c.nome_categoria",
    "c.descricao",
    "p.preco",
    "p.ativo",
    "p.ingestion_ts"
)

# ======================================================
# HASH
# ======================================================
df = df.withColumn(
    "hash_diff",
    F.sha2(
        F.concat_ws("||",
            "nome_categoria",
            "nome_produto",
            "descricao",
            "preco",
            "ativo"
        ),
        256
    )
)

# ======================================================
# COLUMNS SCD2
# ======================================================
df = df.withColumn("dt_inicio", F.col("ingestion_ts")) \
       .withColumn("dt_fim", F.lit(None).cast("timestamp")) \
       .withColumn("is_current", F.lit(True))

# =====================================================
# SK
# =====================================================
df = df.withColumn(
    "sk_produto",
    F.abs(F.hash("id_produto", "dt_inicio")).cast("bigint")
)

# ======================================================
# FINAL SELECT
# ======================================================
df = df.select(
    "id_produto",
    "sk_produto",
    "nome_produto",
    "nome_categoria",
    "descricao",
    "preco",
    "ativo",
    "hash_diff",
    "dt_inicio",
    "dt_fim",
    "is_current"
)

# ======================================================
# WRITE (SCD2 MERGE)
# ======================================================
if not DeltaTable.isDeltaTable(spark, refined):

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(refined)
    )

else:

    delta = DeltaTable.forPath(spark, refined)

    # ==================================================
    # 1. EXPIRAR REGISTROS ATUAIS (CHANGE DETECTION)
    # ==================================================
    (
        delta.alias("t")
        .merge(
            df.alias("s"),
            "t.id_produto = s.id_produto AND t.is_current = true"
        )
        .whenMatchedUpdate(
            condition="t.hash_diff <> s.hash_diff",
            set={
                "dt_fim": "s.dt_inicio",
                "is_current": "false"
            }
        )
        .execute()
    )

    # ==================================================
    # 2. IDENTIFICAR NOVOS / ALTERADOS (CDC EXPLÍCITO)
    # ==================================================
    df_new = (
        df.alias("s")
        .join(
            delta.toDF().alias("t"),
            (F.col("s.id_produto") == F.col("t.id_produto")) &
            (F.col("t.is_current") == True),
            "left"
        )
        .where(
            (F.col("t.id_produto").isNull()) |  # novo
            (F.col("t.hash_diff") != F.col("s.hash_diff"))  # alterado
        )
        .select("s.*")
    )

    # ==================================================
    # 3. INSERIR NOVAS VERSÕES
    # ==================================================
    (
        delta.alias("t")
        .merge(
            df_new.alias("s"),
            "t.id_produto = s.id_produto AND t.is_current = true"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

print("[DIM_PRODUTO] OK")

spark.stop()