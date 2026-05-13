# ======================================================
# IMPORTS
# ======================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# =====================================================
# PATHS
# =====================================================
trusted_products_path = "/data/03_trusted/ecommerce/produtos"
trusted_categories_path = "/data/03_trusted/ecommerce/categorias"
refined_product_dimension_path = "/data/04_refined/ecommerce/dim_produto"

# =====================================================
# SPARK SESSION DELTA
# =====================================================
spark = (
    SparkSession.builder
    .appName("refined_dim_produto")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
    .getOrCreate()
)

# =====================================================
# READ TRUSTED
# =====================================================
trusted_products_df = (
    spark.read
    .format("delta")
    .load(trusted_products_path)
)

trusted_categories_df = (
    spark.read
    .format("delta")
    .load(trusted_categories_path)
)

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
    .select(
        F.col("p.id_produto"),
        F.col("p.nome_produto"),
        F.col("c.nome_categoria"),
        F.col("c.descricao"),
        F.col("p.preco"),
        F.col("p.ativo"),
        F.col("p.ingestion_ts")
    )
)

# ======================================================
# HASH DIFF
# ======================================================
scd2_products_df = product_dimension_base_df.withColumn(
    "hash_diff",
    F.sha2(
        F.concat_ws(
            "||",
            F.col("nome_categoria"),
            F.col("nome_produto"),
            F.col("descricao"),
            F.col("preco").cast("string"),
            F.col("ativo").cast("string")
        ),
        256
    )
)

# ======================================================
# SCD2 COLUMNS
# ======================================================
scd2_products_df = (
    scd2_products_df
    .withColumn("dt_inicio", F.col("ingestion_ts"))
    .withColumn("dt_fim", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

# =====================================================
# SURROGATE KEY
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

    dim_products_delta_table = DeltaTable.forPath(
        spark,
        refined_product_dimension_path
    )

    # ==================================================
    # 1. EXPIRAR REGISTROS ATUAIS
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
    # 2. IDENTIFICAR NOVAS VERSÕES
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
            (F.col("t.id_produto").isNull()) |
            (F.col("t.hash_diff") != F.col("s.hash_diff"))
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

# ======================================================
# HIVE METASTORE REGISTRATION
# ======================================================
spark.sql("SHOW DATABASES").show(truncate=False)

spark.sql("""
CREATE DATABASE IF NOT EXISTS refined
LOCATION 'hdfs://namenode:8020/data/warehouse/refined.db'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS refined.dim_produto
USING DELTA
LOCATION 'hdfs://namenode:8020/data/04_refined/ecommerce/dim_produto'
""")

print("[DIM_PRODUTO] OK")

spark.stop()