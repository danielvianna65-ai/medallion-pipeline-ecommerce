# ======================================================
# IMPORTS
# ======================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# =====================================================
# PATHS
# =====================================================
trusted_payments_path = "/data/03_trusted/ecommerce/pagamentos"
refined_payment_dimension_path = "/data/04_refined/ecommerce/dim_pagamento"

# =====================================================
# SPARK SESSION DELTA
# =====================================================
spark = (
    SparkSession.builder
    .appName("refined_dim_pagamento")
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
trusted_payments_df = (
    spark.read
    .format("delta")
    .load(trusted_payments_path)
)

# =====================================================
# SURROGATE KEY
# =====================================================
payment_dimension_base_df = trusted_payments_df.withColumn(
    "sk_pagamento",
    F.abs(F.hash("id_pagamento")).cast("bigint")
)

# =====================================================
# SELECT
# =====================================================
refined_payments_df = payment_dimension_base_df.select(
    "id_pagamento",
    "id_pedido",
    "sk_pagamento",
    "forma_pagamento",
    "status_pagamento",
    "valor_pago",
    "data_transacao"
)

# =====================================================
# WRITE / MERGE (SCD1)
# =====================================================
if not DeltaTable.isDeltaTable(spark, refined_payment_dimension_path):

    (
        refined_payments_df.write
        .format("delta")
        .mode("overwrite")
        .save(refined_payment_dimension_path)
    )

else:

    dim_payments_delta_table = DeltaTable.forPath(
        spark,
        refined_payment_dimension_path
    )

    (
        dim_payments_delta_table.alias("t")
        .merge(
            refined_payments_df.alias("s"),
            "t.id_pagamento = s.id_pagamento"
        )
        .whenMatchedUpdate(set={
            "id_pedido": "s.id_pedido",
            "forma_pagamento": "s.forma_pagamento",
            "status_pagamento": "s.status_pagamento",
            "valor_pago": "s.valor_pago",
            "data_transacao": "s.data_transacao"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

# =====================================================
# HIVE METASTORE REGISTRATION
# =====================================================
spark.sql("SHOW DATABASES").show(truncate=False)

spark.sql("""
CREATE DATABASE IF NOT EXISTS refined
LOCATION 'hdfs://namenode:8020/data/warehouse/refined.db'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS refined.dim_pagamento
USING DELTA
LOCATION 'hdfs://namenode:8020/data/04_refined/ecommerce/dim_pagamento'
""")

print("[DIM_PAGAMENTO] OK")

spark.stop()