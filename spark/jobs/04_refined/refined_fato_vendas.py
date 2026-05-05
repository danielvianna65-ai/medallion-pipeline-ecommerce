# ======================================================
# IMPORTS
# ======================================================
from itertools import count

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# ======================================================
# PATHS
# ======================================================
base = "/data/03_trusted/ecommerce"
refined = "/data/04_refined/ecommerce"
fato_path = f"{refined}/fato_vendas"

# dimensões
dim_cliente_path = f"{refined}/dim_cliente"
dim_produto_path = f"{refined}/dim_produto"
dim_pagamento_path = f"{refined}/dim_pagamento"
dim_data_path = f"{refined}/dim_data"

# ======================================================
# SPARK SESSION
# ======================================================
spark = (
    SparkSession.builder
    .appName("fato_vendas")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


# ======================================================
# READ TRUSTED
# ======================================================
itens = spark.read.format("delta").load(f"{base}/itens_pedido")

w = Window.partitionBy("id_pedido").orderBy(
    F.col("data_pagamento").desc(),
    F.col("id_pagamento").desc()
)

pag = (
    spark.read.format("delta").load(f"{base}/pagamentos")
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)
pagamentos_trusted = spark.read.format("delta").load(f"{base}/pagamentos")
print("Contagem total de pagamentos na trusted:", pagamentos_trusted.count())
print("Contagem de pagamentos após a lógica de row_number:", pag.count())

w_ped = Window.partitionBy("id_pedido").orderBy(
    F.col("data_transacao").desc(),
    F.col("ingestion_ts").desc()
)

ped = (
    spark.read.format("delta").load(f"{base}/pedidos")
    .withColumn("rn", F.row_number().over(w_ped))
    .filter("rn = 1")
    .drop("rn")
)
pedidos_trusted = spark.read.format("delta").load(f"{base}/pedidos")
print("Contagem total de pedidos na trusted:", pedidos_trusted.count())
print("Contagem de pedidos após a lógica de row_number:", ped.count())

# ======================================================
# READ DIMENSIONS
# ======================================================
dim_cliente = spark.read.format("delta").load(dim_cliente_path)
dim_produto = spark.read.format("delta").load(dim_produto_path)
dim_pagamento = spark.read.format("delta").load(dim_pagamento_path)
dim_data = spark.read.format("delta").load(dim_data_path)

# ======================================================
# JOIN
# ======================================================
df = (
    itens.alias("i")
    .join(ped.alias("p"), "id_pedido")
    .join(pag.alias("pg"), "id_pedido", "left")
)

# ======================================================
# DEBUG - PERDA NO JOIN
# ======================================================

itens_ids = itens.select("id_item_pedido")

df_join_ids = df.select("id_item_pedido")

perdidos_join = itens_ids.subtract(df_join_ids)

print("[DEBUG JOIN]")
print(f"Total itens original: {itens_ids.count()}")
print(f"Total após join: {df_join_ids.count()}")
print(f"Registros perdidos no join: {perdidos_join.count()}")

perdidos_join.show()

df = df.select(
    "i.id_pedido",
    "i.id_produto",
    "i.id_item_pedido",
    "p.id_cliente",
    "pg.id_pagamento",
    F.date_trunc("second", F.col("p.data_transacao")).alias("dt_pedido"),
    "i.quantidade",
    "i.preco_unitario"
)

df = df.withColumn(
    "dt_carga",
    F.current_timestamp()
)

# ======================================================
# JOIN DIMENSÕES
# ======================================================

# cliente (SCD2 simplificado - current)
dim_cliente = dim_cliente.filter("is_current = true")

df = df.join(
    F.broadcast(dim_cliente.select("id_cliente", "sk_cliente")),
    "id_cliente",
    "left"
)

# produto
dim_produto = dim_produto.filter("is_current = true")

df = df.join(
    F.broadcast(dim_produto.select("id_produto", "sk_produto")),
    "id_produto",
    "left"
)

# pagamento
df = df.join(
    F.broadcast(dim_pagamento.select("id_pagamento", "sk_pagamento")),
    "id_pagamento",
    "left"
)

# data
df = df.withColumn(
    "sk_data_pedido",
    F.date_format("dt_pedido", "yyyyMMdd").cast("int")
)

erros = df.join(
    dim_data.select("sk_data"),
    df.sk_data_pedido == dim_data.sk_data,
    "left_anti"
).count()

if erros > 0:
    raise Exception(f"Erro de integridade na dim_data: {erros} registros inválidos")

# ======================================================
# DATA QUALITY (REFATORADO)
# ======================================================

df_invalid = df.filter(
    F.col("sk_cliente").isNull() |
    F.col("sk_produto").isNull() |
    F.col("sk_pagamento").isNull() |
    F.col("sk_data_pedido").isNull()
)

df_valid = df.filter(
    F.col("sk_cliente").isNotNull() &
    F.col("sk_produto").isNotNull() &
    F.col("sk_pagamento").isNotNull() &
    F.col("sk_data_pedido").isNotNull()
)

# ======================================================
# LOG (observabilidade)
# ======================================================

total = df.count()
validos = df_valid.count()
invalidos = df_invalid.count()

print(f"[DATA QUALITY]")
print(f"Total entrada: {total}")
print(f"Registros válidos: {validos}")
print(f"Registros rejeitados: {invalidos}")

# ======================================================
# (OPCIONAL) SALVAR REJEITADOS
# ======================================================

if invalidos > 0:
    df_invalid.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/data/04_refined/ecommerce/rejected_fato_vendas")

# ======================================================
# SEGUE PIPELINE COM df_valid
# ======================================================

df = df_valid

# ======================================================
# MÉTRICAS
# ======================================================
df = df.withColumn(
    "valor_total_item",
    (F.col("quantidade") * F.col("preco_unitario")).cast("decimal(12,2)")
)

# ======================================================
# SK
# ======================================================
df = df.withColumn(
    "sk_venda",
    F.sha2(
        F.concat_ws("|", "id_pedido", "id_item_pedido", "id_produto",),
        256
    )
)

# ======================================================
# FINAL SELECT
# ======================================================
df_final = df.select(
    "sk_venda",
    "sk_cliente",
    "sk_produto",
    "sk_pagamento",
    "sk_data_pedido",
    "dt_pedido",
    "id_pedido",
    "id_item_pedido",
    "quantidade",
    "preco_unitario",
    "valor_total_item",
    "dt_carga"
)

# ======================================================
# DATA QUALITY - DUPLICIDADE
# ======================================================
duplicados = df.groupBy("sk_venda").count().filter("count > 1")

if duplicados.count() > 0:
    print("[ERRO] Duplicidade de sk_venda detectada")
    duplicados.show()
    raise Exception("Duplicidade de chave na fato")

# ======================================================
# WRITE (UPSERT REAL)
# ======================================================
if not DeltaTable.isDeltaTable(spark, fato_path):

    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .save(fato_path)

else:

    delta = DeltaTable.forPath(spark, fato_path)

    (
        delta.alias("t")
        .merge(df_final.alias("s"), "t.sk_venda = s.sk_venda")

        .whenMatchedUpdate(
            condition="""
            NOT (t.sk_cliente <=> s.sk_cliente) OR
            NOT (t.sk_produto <=> s.sk_produto) OR
            NOT (t.sk_pagamento <=> s.sk_pagamento) OR
            NOT (t.sk_data_pedido <=> s.sk_data_pedido) OR
            NOT (t.dt_pedido <=> s.dt_pedido) OR
            NOT (t.id_pedido <=> s.id_pedido) OR
            NOT (t.id_item_pedido <=> s.id_item_pedido) OR
            NOT (t.quantidade <=> s.quantidade) OR
            NOT (t.preco_unitario <=> s.preco_unitario) OR
            NOT (t.valor_total_item <=> s.valor_total_item)
            """,
            set={
                "sk_cliente": "s.sk_cliente",
                "sk_produto": "s.sk_produto",
                "sk_pagamento": "s.sk_pagamento",
                "sk_data_pedido": "s.sk_data_pedido",
                "dt_pedido": "s.dt_pedido",
                "id_pedido": "s.id_pedido",
                "id_item_pedido": "s.id_item_pedido",
                "quantidade": "s.quantidade",
                "preco_unitario": "s.preco_unitario",
                "valor_total_item": "s.valor_total_item",
                "dt_carga": "s.dt_carga"
            }
        )

        .whenNotMatchedInsertAll()
        .execute()
    )

print("[FATO_VENDAS] OK")

spark.stop()