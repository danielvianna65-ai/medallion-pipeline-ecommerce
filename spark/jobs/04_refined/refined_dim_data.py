from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder
    .appName("dim_data")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

refined_path = "/data/04_refined/ecommerce/dim_data"

df = spark.sql("""
SELECT sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day) as data_seq
""").select(F.explode("data_seq").alias("data"))

df = df.select(
    F.date_format("data", "yyyyMMdd").cast("int").alias("sk_data"),
    "data",
    F.year("data").alias("ano"),
    F.month("data").alias("mes"),
    F.dayofmonth("data").alias("dia"),
    F.date_format("data", "yyyy-MM").alias("ano_mes"),
    F.dayofweek("data").alias("dia_semana"),
    F.quarter("data").alias("trimestre"),
    F.weekofyear("data").alias("semana_ano"),
    F.when(F.dayofweek("data").isin([1,7]), True).otherwise(False).alias("fim_de_semana")
)

df = df.withColumn(
    "nome_dia",
    F.when(F.col("dia_semana") == 1, "Domingo")
     .when(F.col("dia_semana") == 2, "Segunda")
     .when(F.col("dia_semana") == 3, "Terça")
     .when(F.col("dia_semana") == 4, "Quarta")
     .when(F.col("dia_semana") == 5, "Quinta")
     .when(F.col("dia_semana") == 6, "Sexta")
     .when(F.col("dia_semana") == 7, "Sábado")
)

df = df.withColumn(
    "dia_util",
    ~F.col("dia_semana").isin([1, 7])
)

df = df.orderBy("data")
df.write.format("delta").mode("overwrite").save(refined_path)

spark.stop()