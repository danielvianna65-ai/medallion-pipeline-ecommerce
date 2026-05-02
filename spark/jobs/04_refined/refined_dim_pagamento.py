# ======================================================
# IMPORTS
# ======================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# =====================================================
# Paths
# =====================================================
trusted = "/data/03_trusted/ecommerce/pagamentos"
refined = "/data/04_refined/ecommerce/dim_pagamento"

# =====================================================
# Spark Session Delta
# =====================================================
spark = (
    SparkSession.builder
    .appName("dim_pagamento")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# =====================================================
# READ TRUSTED
# =====================================================
df = spark.read.format("delta").load(trusted)

# =====================================================
# SK
# =====================================================
df = df.withColumn(
    "sk_pagamento",
    F.abs(F.hash("id_pagamento")).cast("bigint")
)

# =====================================================
# SELECT
# =====================================================
df = df.select(
    "id_pagamento",
    "id_pedido",
    "sk_pagamento",
    "forma_pagamento",
    "status_pagamento",
    "valor_pago",
    "data_transacao"
)

# =====================================================
# MERGE SCD1
# =====================================================
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