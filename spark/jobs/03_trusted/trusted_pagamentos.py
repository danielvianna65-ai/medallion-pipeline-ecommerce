from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, date_format

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("TRUSTED - Pagamentos")
    .enableHiveSupport()
    .config(
            "spark.sql.warehouse.dir",
            "hdfs://namenode:8020/user/hive/warehouse"
        )
    .getOrCreate()
)

# -----------------------------
# Paths
# -----------------------------
raw_path = "hdfs://namenode:8020/data/raw/ecommerce/pagamentos"
trusted_path = "hdfs://namenode:8020/data/trusted/ecommerce/pagamentos"

# -----------------------------
# Leitura RAW
# -----------------------------
df_raw = spark.read.parquet(raw_path)

# -----------------------------
# Validações estruturais + negócio
# -----------------------------
df_valid = (
    df_raw
    .filter(col("id_pagamento").isNotNull())
    .filter(col("id_pedido").isNotNull())
    .filter(col("metodo_pagamento").isNotNull())
    .filter(col("status").isNotNull())
    .filter(col("valor").isNotNull())
    .filter(col("valor") > 0)
    .filter(col("data_pagamento").isNotNull())
)

# -----------------------------
# Deduplicação
# -----------------------------
df_valid = df_valid.dropDuplicates(["id_pagamento"])

# -----------------------------
# Padronização + metadados Trusted + partição diária
# -----------------------------
df_trusted = (
    df_valid
    .withColumnRenamed("status", "status_pagamento")
    .withColumnRenamed("valor", "valor_pagamento")
    .withColumn("trusted_ts", current_timestamp())
    .withColumn("trusted_version", lit(1))
    .withColumn("dt", date_format(col("data_pagamento"), "yyyy-MM-dd"))
)

# -----------------------------
# Escrita Trusted (ingestão diária)
# -----------------------------
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df_trusted
    .write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet(trusted_path)
)

# -----------------------------
# Metastore
# -----------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS trusted_ecommerce")

spark.sql("""
CREATE TABLE IF NOT EXISTS trusted_ecommerce.pagamentos (
    id_pagamento INT,
    id_pedido INT,
    metodo_pagamento STRING,
    status_pagamento STRING,
    valor_pagamento DECIMAL(12,2),
    data_pagamento TIMESTAMP,
    ingestion_ts TIMESTAMP,
    source_system STRING,
    dt STRING,
    trusted_ts TIMESTAMP,
    trusted_version INT
)
USING PARQUET
PARTITIONED BY (dt)
LOCATION 'hdfs://namenode:8020/data/trusted/ecommerce/pagamentos'
""")

spark.sql("MSCK REPAIR TABLE trusted_ecommerce.pagamentos")

print("✅ TRUSTED pagamentos processada com sucesso")

spark.stop()
