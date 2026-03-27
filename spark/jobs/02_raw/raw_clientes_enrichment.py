# =====================================================
# IMPORTS
# =====================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType

# ======================================================
# SPARK SESSION
# ======================================================
spark = (
    SparkSession.builder
    .appName("raw_clientes_enrichment")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# =====================================================
# PATHS
# =====================================================
table = "clientes_enrichment"

landing_path = f"/data/01_landing/ecommerce/{table}"
raw_path = f"/data/02_raw/ecommerce/{table}"

# ======================================================
# READ LANDING
# ======================================================
print(f"[RAW][{table}] Source: {landing_path}")
print(f"[RAW][{table}] Target: {raw_path}")

df_inc = (
        spark.read
        .option("mode", "FAILFAST")
        .parquet(landing_path)
)

# =====================================================
# DROP DUPLICATES (CPF)
# =====================================================
df_inc = df_inc.dropDuplicates(["cpf"])

# =====================================================
# SCHEMA
# =====================================================
df_inc = (
    spark.read.parquet(landing_path)
    .select(
        col("cpf").cast("string"),
        col("nome").cast("string"),
        col("email").cast("string"),
        col("telefone").cast("string"),
        col("renda_estimada").cast("decimal(10,2)"),
        col("score_credito").cast("int"),
        col("dt")
    )
)
print(f"[RAW][{table}] Schema inferido explicitamente")
df_inc.printSchema()

# =====================================================
# METADATA ENRICHMENT
# =====================================================
df_inc = (
    df_inc
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source_system", lit("partner_api"))
)

# =====================================================
# BOOTSTRAP
# =====================================================

is_bootstrap = not DeltaTable.isDeltaTable(spark, raw_path)

if is_bootstrap:

    print(f"[RAW][{table}] Primeira carga → criando Delta")

    (
        df_inc.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("dt")
        .save(raw_path)
    )

# ======================================================
# INCREMENTAL MERGE
# ======================================================

else:

    print(f"[RAW][{table}] Executando MERGE")

    print(f"[RAW][{table}] Qtd registros antes do merge:",
          spark.read.format("delta").load(raw_path).count())

    delta_table = DeltaTable.forPath(spark, raw_path)

    update_set = {
        "nome": "source.nome",
        "email": "source.email",
        "telefone": "source.telefone",
        "renda_estimada": "source.renda_estimada",
        "score_credito": "source.score_credito",
        "ingestion_ts": "source.ingestion_ts"
    }

    insert_set = {
        "cpf": "source.cpf",
        "nome": "source.nome",
        "email": "source.email",
        "telefone": "source.telefone",
        "renda_estimada": "source.renda_estimada",
        "score_credito": "source.score_credito",
        "ingestion_ts": "source.ingestion_ts"
    }

    (
        delta_table.alias("target")
        .merge(
            df_inc.alias("source"),
            "target.cpf = source.cpf"
        )
        .whenMatchedUpdate(
            condition="""
                     COALESCE(target.nome, '') <> COALESCE(source.nome, '') OR
                     COALESCE(target.email, '') <> COALESCE(source.email, '') OR
                     COALESCE(target.telefone, '') <> COALESCE(source.telefone, '') OR
                     COALESCE(target.renda_estimada, '') <> COALESCE(source.renda_estimada, '') OR
                     COALESCE(target.score_credito, '') <> COALESCE(source.score_credito, '')
                 """,
            set=update_set
        )

        .whenNotMatchedInsert(values=insert_set)

        .execute()
    )
print(f"[RAW][{table}] MERGE concluído.")

print(f"[RAW][{table}] Qtd registros após o merge:",
      spark.read.format("delta").load(raw_path).count())

spark.stop()