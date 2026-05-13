# ======================================================
# IMPORTS
# ======================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ======================================================
# PATH
# ======================================================
refined_path = "/data/04_refined/ecommerce/dim_data"

# =====================================================
# Spark Session Delta
# =====================================================
spark = (
    SparkSession.builder
    .appName("refined_dim_data")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
    .getOrCreate()
)

# ======================================================
# GENERATE DATE RANGE (BASE DATASET)
# ======================================================
calendar_base_df = (
    spark.sql("""
        SELECT sequence(
            to_date('2020-01-01'),
            to_date('2030-12-31'),
            interval 1 day
        ) as data_seq
    """)
    .select(F.explode("data_seq").alias("data"))
)

# ======================================================
# DERIVE DATE ATTRIBUTES (CALENDAR DIMENSIONS)
# ======================================================
calendar_attributes_df = calendar_base_df.select(
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

# ======================================================
# DERIVE BUSINESS ATTRIBUTES (SEMANTIC LAYER)
# ======================================================
business_calendar_df = (
    calendar_attributes_df
    .withColumn(
        "nome_dia",
        F.when(F.col("dia_semana") == 1, "Domingo")
         .when(F.col("dia_semana") == 2, "Segunda")
         .when(F.col("dia_semana") == 3, "Terça")
         .when(F.col("dia_semana") == 4, "Quarta")
         .when(F.col("dia_semana") == 5, "Quinta")
         .when(F.col("dia_semana") == 6, "Sexta")
         .when(F.col("dia_semana") == 7, "Sábado")
    )
    .withColumn(
        "dia_util",
        ~F.col("dia_semana").isin([1, 7])
    )
)

# ======================================================
# FINAL ORDERING
# ======================================================
refined_date_dimension_df = business_calendar_df.orderBy("data")

# ======================================================
# WRITE (DELTA - OVERWRITE)
# ======================================================
(
    refined_date_dimension_df.write
    .format("delta")
    .mode("overwrite")
    .save(refined_path)
)

# ======================================================
# HIVE METASTORE REGISTRATION
# ======================================================
spark.sql("SHOW DATABASES").show(truncate=False)

spark.sql("""
CREATE DATABASE IF NOT EXISTS refined
LOCATION 'hdfs://namenode:8020/data/warehouse/refined.db'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS refined.dim_data
USING DELTA
LOCATION 'hdfs://namenode:8020/data/04_refined/ecommerce/dim_data'
""")

print("[DIM_DATA] OK")

spark.stop()