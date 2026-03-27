from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("dim_produto_scd2")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

prod = "/data/03_trusted/ecommerce/produtos"
cat = "/data/04_refined/ecommerce/dim_categoria"
refined = "/data/04_refined/ecommerce/dim_produto"

# =========================
# LEITURA
# =========================
df_prod = spark.read.format("delta").load(prod)
dim_cat = spark.read.format("delta").load(cat)

# =========================
# JOIN + SELEÇÃO
# =========================
df = (
    df_prod.alias("p")
    .join(
        dim_cat.alias("c"),
        F.col("p.id_categoria") == F.col("c.id_categoria"),
        "left"
    )
)

df = df.select(
    F.col("p.id_produto"),
    F.col("c.sk_categoria"),
    F.col("p.nome_produto"),
    F.col("p.descricao"),
    F.col("p.preco"),
    F.col("p.ativo")
)

df = df.dropDuplicates(["id_produto"])

# =========================
# HASH PARA DETECTAR MUDANÇA
# =========================
df = df.withColumn(
    "hash_diff",
    F.sha2(
        F.concat_ws("||",
            "sk_categoria",
            "nome_produto",
            "descricao",
            "preco",
            "ativo"
        ),
        256
    )
)

# =========================
# CONTROLE SCD2
# =========================
df = df.withColumn("dt_inicio", F.current_date()) \
       .withColumn("dt_fim", F.lit(None).cast("date")) \
       .withColumn("is_current", F.lit(True))

# =========================
# SK (agora precisa mudar por versão)
# =========================
df = df.withColumn(
    "sk_produto",
    F.abs(F.hash("id_produto", "dt_inicio")).cast("bigint")
)

# =========================
# CARGA INICIAL
# =========================
if not DeltaTable.isDeltaTable(spark, refined):
    df.write.format("delta").mode("overwrite").save(refined)

else:
    delta = DeltaTable.forPath(spark, refined)

    # =========================
    # 1. EXPIRAR REGISTROS ANTIGOS
    # =========================
    delta.alias("t").merge(
        df.alias("s"),
        "t.id_produto = s.id_produto AND t.is_current = true"
    ).whenMatchedUpdate(
        condition="t.hash_diff <> s.hash_diff",
        set={
            "dt_fim": "current_date()",
            "is_current": "false"
        }
    ).execute()

    # =========================
    # 2. INSERIR NOVAS VERSÕES
    # =========================
    delta.alias("t").merge(
        df.alias("s"),
        "t.id_produto = s.id_produto AND t.hash_diff = s.hash_diff AND t.is_current = true"
    ).whenNotMatchedInsertAll().execute()

spark.stop()