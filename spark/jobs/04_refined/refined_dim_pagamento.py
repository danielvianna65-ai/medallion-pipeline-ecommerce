from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("dim_pagamento")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

trusted = "/data/03_trusted/ecommerce/pagamentos"
refined = "/data/04_refined/ecommerce/dim_pagamento"

df = spark.read.format("delta").load(trusted)

# =========================
# SELEÇÃO CORRETA (SÓ DIMENSÃO)
# =========================
df = df.select(
    "id_pagamento",
    F.trim(F.col("forma_pagamento")).alias("forma_pagamento"),
    F.trim(F.col("status_pagamento")).alias("status_pagamento")
)

# remove duplicidade por chave de negócio
df = df.dropDuplicates(["id_pagamento"])

# =========================
# SK ESTÁVEL
# =========================
df = df.withColumn(
    "sk_pagamento",
    F.abs(F.hash("id_pagamento")).cast("bigint")
)

# =========================
# MERGE SCD1
# =========================
if not DeltaTable.isDeltaTable(spark, refined):
    df.write.format("delta").mode("overwrite").save(refined)

else:
    DeltaTable.forPath(spark, refined) \
        .alias("t") \
        .merge(df.alias("s"), "t.id_pagamento = s.id_pagamento") \
        .whenMatchedUpdate(set={
            "forma_pagamento": "s.forma_pagamento",
            "status_pagamento": "s.status_pagamento"
        }) \
        .whenNotMatchedInsertAll() \
        .execute()

spark.stop()