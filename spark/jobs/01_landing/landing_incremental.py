# =====================================================
# IMPORTS
# =====================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, to_date
import argparse

# ==========================
# Args vindos do Airflow
# ==========================
parser = argparse.ArgumentParser()

parser.add_argument("--table", required=True)
parser.add_argument("--watermark_col", default="data_transacao")
parser.add_argument("--landing_base", required=True)
parser.add_argument("--jdbc_url", required=True)
parser.add_argument("--jdbc_user", required=True)
parser.add_argument("--jdbc_password", required=True)
parser.add_argument("--execution_date", required=True)

# =====================================================
# Paths
# =====================================================
args = parser.parse_args()

table = args.table
watermark_col = args.watermark_col
execution_date = args.execution_date

landing_path = f"{args.landing_base}/{table}"
watermark_path = f"{args.landing_base}/_watermarks/{table}"

# ==========================
# Spark Session
# ==========================
spark = (
    SparkSession.builder
    .appName(f"landing_incremental_{table}")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.hadoop.dfs.replication", "1")
    .getOrCreate()
)


print(f"[INFO] Tabela: {table}")
print(f"[INFO] Execution date: {execution_date}")

# ==========================
# 1) Ler watermark do metadata
# ==========================
stored_watermark = None

try:
    stored_watermark_df = spark.read.json(watermark_path)

    stored_watermark = (
        stored_watermark_df
        .agg(spark_max(col("watermark")).alias("wm"))
        .collect()[0]["wm"]
    )

    print(f"[INFO] Watermark metadata encontrado: {stored_watermark}")

except Exception:
    print("[INFO] Primeira execução - watermark ainda não existe.")

# ==========================
# 2) Montar query incremental
# ==========================
if stored_watermark:
    jdbc_extraction_query = f"""
        (SELECT *
         FROM {table}
         WHERE {watermark_col} > TIMESTAMP('{stored_watermark}')) AS inc
    """
else:
    jdbc_extraction_query = f"(SELECT * FROM {table}) AS full"

print("[INFO] Query de extração JDBC montada.")

# ==========================
# 3) Ler JDBC incremental
# ==========================
jdbc_connection_properties = {
    "user": args.jdbc_user,
    "password": args.jdbc_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

incremental_extract_df = (
    spark.read
    .format("jdbc")
    .option("url", args.jdbc_url)
    .option("dbtable", jdbc_extraction_query)
    .options(**jdbc_connection_properties)
    .load()
)

# ==========================
# 4) Check vazio leve
# ==========================
if incremental_extract_df.rdd.isEmpty():
    print("[INFO] Nenhum dado novo.")
    spark.stop()
    exit(0)

# ==========================
# 5) Adicionar partição dt
# ==========================
incremental_partitioned_df = incremental_extract_df.withColumn(
    "dt",
    to_date(col(watermark_col))
)

# ==========================
# 7) Escrever incremental na LANDING
# ==========================
(
    incremental_partitioned_df.write
    .mode("append")
    .partitionBy("dt")
    .parquet(landing_path)
)

print(f"[INFO] Ingestão incremental concluída para {table}")

# ==========================
# 8) Atualizar watermark metadata
# ==========================
latest_incremental_watermark = (
    incremental_partitioned_df
    .agg(spark_max(col(watermark_col)).alias("max_ts"))
    .collect()[0]["max_ts"]
)

if latest_incremental_watermark:
    print(f"[INFO] Novo watermark calculado: {latest_incremental_watermark}")

    spark.createDataFrame(
        [(str(latest_incremental_watermark),)],
        ["watermark"]
    ).coalesce(1).write.mode("overwrite").json(watermark_path)

    print("[INFO] Watermark metadata atualizado.")

print("[LANDING] Qtd registros após o merge:",
      spark.read.parquet(landing_path).count()
)

spark.stop()
