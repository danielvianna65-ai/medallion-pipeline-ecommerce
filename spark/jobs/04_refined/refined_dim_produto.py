# ======================================================
# IMPORTS
# ======================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# =====================================================
# Paths
# =====================================================
trusted_products_path = "/data/03_trusted/ecommerce/produtos"
trusted_categories_path = "/data/03_trusted/ecommerce/categorias"
refined_product_dimension_path = "/data/04_refined/ecommerce/dim_produto"

# =====================================================
# Spark Session Delta
# =====================================================
spark = (
    SparkSession.builder
    .appName("refined_dim_produto")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# =====================================================
# READ TRUSTED/REFINED
# =====================================================
trusted_products_df = spark.read.format("delta").load(trusted_products_path)
trusted_categories_df = spark.read.format("delta").load(trusted_categories_path)

# =====================================================
# JOIN
# =====================================================
product_dimension_base_df = (
    trusted_products_df.alias("p")
    .join(
        trusted_categories_df.alias("c"),
        F.col("p.id_categoria") == F.col("c.id_categoria"),
        "left"
    )
)

# =====================================================
# SELECT / JOIN
# =====================================================
product_dimension_base_df = product_dimension_base_df.select(
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
scd2_products_df = product_dimension_base_df.withColumn(
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
scd2_products_df = scd2_products_df.withColumn("dt_inicio", F.col("ingestion_ts")) \
       .withColumn("dt_fim", F.lit(None).cast("timestamp")) \
       .withColumn("is_current", F.lit(True))

# =====================================================
# SK
# =====================================================
scd2_products_df = scd2_products_df.withColumn(
    "sk_produto",
    F.abs(F.hash("id_produto", "dt_inicio")).cast("bigint")
)

# ======================================================
# FINAL SELECT
# ======================================================
refined_products_df = scd2_products_df.select(
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
if not DeltaTable.isDeltaTable(spark, refined_product_dimension_path):

    (
        refined_products_df.write
        .format("delta")
        .mode("overwrite")
        .save(refined_product_dimension_path)
    )

else:

    dim_products_delta_table = DeltaTable.forPath(spark, refined_product_dimension_path)

    # ==================================================
    # 1. EXPIRAR REGISTROS ATUAIS (CHANGE DETECTION)
    # ==================================================
    (
        dim_products_delta_table.alias("t")
        .merge(
            refined_products_df.alias("s"),
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
    new_product_versions_df = (
        refined_products_df.alias("s")
        .join(
            dim_products_delta_table.toDF().alias("t"),
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
        dim_products_delta_table.alias("t")
        .merge(
            new_product_versions_df.alias("s"),
            "t.id_produto = s.id_produto AND t.is_current = true"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

print("[DIM_PRODUTO] OK")

spark.stop()