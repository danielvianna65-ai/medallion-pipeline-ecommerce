import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--table", required=True)
parser.add_argument("--execution_date", required=True)
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
print(f"📅 Execution date: {args.execution_date}")

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

output_path = (
    f"hdfs://namenode:8020/data/landing/ecommerce/"
    f"{args.table}/dt={args.execution_date}"
)

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print(f"✅ Tabela {args.table} gravada com sucesso em {output_path}")

spark.stop()
