import argparse
from datetime import date
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--table", required=True)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName(f"Landing-{args.table}")
    .getOrCreate()
)

jdbc_url = (
    "jdbc:mysql://172.17.0.1:3306/ecommerce"
    "?useSSL=false"
    "&allowPublicKeyRetrieval=true"
)

props = {
    "user": "spark",
    "password": "Spark@123#2026",
    "driver": "com.mysql.cj.jdbc.Driver",
}

print(f"📥 Iniciando ingestão da tabela: {args.table}")

df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", args.table)
    .option("user", props["user"])
    .option("password", props["password"])
    .option("driver", props["driver"])
    .load()
)

dt = date.today().isoformat()

(
    df.write
    .mode("append")
    .parquet(
        f"hdfs://namenode:8020/data/landing/ecommerce/{args.table}/dt={dt}"
    )
)

print(f"✅ Tabela {args.table} gravada com sucesso no HDFS")

spark.stop()
