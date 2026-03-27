from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# =========================
# SPARK SESSION
# =========================

spark = (
    SparkSession.builder
    .appName("trusted_pagamentos")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

table = "pagamentos"

PRIMARY_KEY = "id_pagamento"
WATERMARK_COL = "data_transacao"

raw_path = f"/data/02_raw/ecommerce/{table}"
trusted_path = f"/data/03_trusted/ecommerce/{table}"

# =========================
# READ RAW
# =========================

df = spark.read.format("delta").load(raw_path)

# =========================
# CLEANING
# =========================

df_clean = df.select(

    F.col("id_pagamento").cast("int"),
    F.col("id_pedido").cast("int"),

    # forma pagamento
    F.lower(F.trim("forma_pagamento")).alias("forma_pagamento"),

    # status pagamento
    F.lower(F.trim("status_pagamento")).alias("status_pagamento"),

    # datas
    F.col("data_pagamento"),

    # valor
    F.col("valor_pago").cast("double"),

    "data_transacao",
    "dt",
    "source_system",
    "ingestion_ts"
)

# =========================
# DATA QUALITY
# =========================

df_clean = (
    df_clean

    # valor válido
    .withColumn(
        "valor_valido",
        F.col("valor_pago") >= 0
    )

    # status válido (exemplo básico)
    .withColumn(
        "status_valido",
        F.col("status_pagamento").isin(
            "aprovado", "recusado", "pendente", "cancelado"
        )
    )
)

# =========================
# AUDITORIA
# =========================

df_clean = df_clean.withColumn(
    "processing_trusted",
    F.current_timestamp()
)

# =========================
# WRITE TRUSTED
# =========================

if not DeltaTable.isDeltaTable(spark, trusted_path):

    print("[Trusted] Bootstrap inicial")

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("dt")
        .save(trusted_path)
    )

else:

    print("[Trusted] Executando MERGE incremental")

    print(
        "[Trusted] Qtd registros antes do merge:",
        spark.read.format("delta").load(trusted_path).count()
    )

    delta_table = DeltaTable.forPath(spark, trusted_path)

    update_set = {c: f"source.{c}" for c in df_clean.columns}
    insert_values = {c: f"source.{c}" for c in df_clean.columns}

    (
        delta_table.alias("target")
        .merge(
            df_clean.alias("source"),
            f"target.{PRIMARY_KEY} = source.{PRIMARY_KEY}"
        )
        .whenMatchedUpdate(
            condition=f"source.{WATERMARK_COL} > target.{WATERMARK_COL}",
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

print("[Trusted] MERGE concluído")

print(
    "[Trusted] Qtd registros após o merge:",
    spark.read.format("delta").load(trusted_path).count()
)

spark.stop()