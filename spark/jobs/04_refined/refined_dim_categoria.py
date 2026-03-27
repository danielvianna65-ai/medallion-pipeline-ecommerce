from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("dim_categoria")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

src = "/data/03_trusted/ecommerce/categorias"
refined = "/data/04_refined/ecommerce/dim_categoria"

df = spark.read.format("delta").load(src)

# =========================
# LIMPEZA E SELEÇÃO
# =========================
df = df.select(
    "id_categoria",
    F.trim(F.col("nome_categoria")).alias("nome_categoria"),
    F.trim(F.col("descricao")).alias("descricao")
)

# remove duplicados por chave de negócio
df = df.dropDuplicates(["id_categoria"])

# =========================
# SK ESTÁVEL
# =========================
df = df.withColumn(
    "sk_categoria",
    F.abs(F.hash("id_categoria")).cast("bigint")
)

# =========================
# MERGE SCD1
# =========================
if not DeltaTable.isDeltaTable(spark, refined):
    df.write.format("delta").mode("overwrite").save(refined)

else:
    DeltaTable.forPath(spark, refined) \
        .alias("t") \
        .merge(df.alias("s"), "t.id_categoria = s.id_categoria") \
        .whenMatchedUpdate(set={
            "nome_categoria": "s.nome_categoria",
            "descricao": "s.descricao"
        }) \
        .whenNotMatchedInsertAll() \
        .execute()

spark.stop()