# ======================================================
# IMPORTS
# ======================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# ======================================================
# PATHS
# ======================================================
trusted_base_path = "/data/03_trusted/ecommerce"
refined_base_path = "/data/04_refined/ecommerce"
sales_fact_path = f"{refined_base_path}/fato_vendas"
rejected_sales_fact_path = "/data/04_refined/ecommerce/rejected_fato_vendas"

# dimensões
dim_cliente_path = f"{refined_base_path}/dim_cliente"
dim_produto_path = f"{refined_base_path}/dim_produto"
dim_pagamento_path = f"{refined_base_path}/dim_pagamento"
dim_data_path = f"{refined_base_path}/dim_data"

# ======================================================
# SPARK SESSION
# ======================================================
spark = (
    SparkSession.builder
    .appName("refined_fato_vendas")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ======================================================
# READ TRUSTED
# ======================================================

trusted_order_items_df = (
    spark.read
    .format("delta")
    .load(f"{trusted_base_path}/itens_pedido")
)

trusted_payments_full_df = (
    spark.read
    .format("delta")
    .load(f"{trusted_base_path}/pagamentos")
)

latest_payment_window = Window.partitionBy("id_pedido").orderBy(
    F.col("data_pagamento").desc(),
    F.col("id_pagamento").desc()
)

latest_payments_df = (
    trusted_payments_full_df
    .withColumn(
        "rn",
        F.row_number().over(latest_payment_window)
    )
    .filter("rn = 1")
    .drop("rn")
)

print(
    "Contagem total de pagamentos na trusted:",
    trusted_payments_full_df.count()
)

print(
    "Contagem de pagamentos após a lógica de row_number:",
    latest_payments_df.count()
)

trusted_orders_full_df = (
    spark.read
    .format("delta")
    .load(f"{trusted_base_path}/pedidos")
)

latest_order_window = Window.partitionBy("id_pedido").orderBy(
    F.col("data_transacao").desc(),
    F.col("ingestion_ts").desc()
)

trusted_orders_df = (
    trusted_orders_full_df
    .withColumn(
        "rn",
        F.row_number().over(latest_order_window)
    )
    .filter("rn = 1")
    .drop("rn")
)

print(
    "Contagem total de pedidos na trusted:",
    trusted_orders_full_df.count()
)

print(
    "Contagem de pedidos após a lógica de row_number:",
    trusted_orders_df.count()
)

# ======================================================
# READ DIMENSIONS
# ======================================================
dim_customers_df = spark.read.format("delta").load(dim_cliente_path)
dim_products_df = spark.read.format("delta").load(dim_produto_path)
dim_payments_df = spark.read.format("delta").load(dim_pagamento_path)
dim_date_df = spark.read.format("delta").load(dim_data_path)

# ======================================================
# JOIN FACT BASE
# ======================================================
sales_fact_base_df = (
    trusted_order_items_df.alias("i")
    .join(
        trusted_orders_df.alias("p"),
        "id_pedido"
    )
    .join(
        latest_payments_df.alias("pg"),
        "id_pedido",
        "left"
    )
)

# ======================================================
# DEBUG - PERDA NO JOIN
# ======================================================

order_item_ids_df = trusted_order_items_df.select("id_item_pedido")

joined_order_item_ids_df = sales_fact_base_df.select("id_item_pedido")

missing_join_records_df = order_item_ids_df.subtract(joined_order_item_ids_df)

print("[DEBUG JOIN]")
print(f"Total itens original: {order_item_ids_df.count()}")
print(f"Total após join: {joined_order_item_ids_df.count()}")
print(f"Registros perdidos no join: {missing_join_records_df.count()}")

missing_join_records_df.show()

# ======================================================
# FACT STAGING
# ======================================================
staged_sales_fact_df = sales_fact_base_df.select(
    "i.id_pedido",
    "i.id_produto",
    "i.id_item_pedido",
    "p.id_cliente",
    "pg.id_pagamento",
    F.date_trunc("second", F.col("p.data_transacao")).alias("dt_pedido"),
    "i.quantidade",
    "i.preco_unitario"
)

staged_sales_fact_df = staged_sales_fact_df.withColumn(
    "dt_carga",
    F.current_timestamp()
)

# ======================================================
# JOIN DIMENSÕES
# ======================================================

# cliente (SCD2 simplificado - current)
dim_customers_df = dim_customers_df.filter("is_current = true")

conformed_sales_fact_df = staged_sales_fact_df.join(
    F.broadcast(
        dim_customers_df.select("id_cliente", "sk_cliente")
    ),
    "id_cliente",
    "left"
)

# produto
dim_products_df = dim_products_df.filter("is_current = true")

conformed_sales_fact_df = conformed_sales_fact_df.join(
    F.broadcast(
        dim_products_df.select("id_produto", "sk_produto")
    ),
    "id_produto",
    "left"
)

# pagamento
conformed_sales_fact_df = conformed_sales_fact_df.join(
    F.broadcast(
        dim_payments_df.select("id_pagamento", "sk_pagamento")
    ),
    "id_pagamento",
    "left"
)

# data
conformed_sales_fact_df = conformed_sales_fact_df.withColumn(
    "sk_data_pedido",
    F.date_format("dt_pedido", "yyyyMMdd").cast("int")
)

invalid_date_keys_count = conformed_sales_fact_df.join(
    dim_date_df.select("sk_data"),
    conformed_sales_fact_df.sk_data_pedido == dim_date_df.sk_data,
    "left_anti"
).count()

if invalid_date_keys_count > 0:
    raise Exception(
        f"Erro de integridade na dim_data: "
        f"{invalid_date_keys_count} registros inválidos"
    )

# ======================================================
# DATA QUALITY
# ======================================================

invalid_sales_fact_df = conformed_sales_fact_df.filter(
    F.col("sk_cliente").isNull() |
    F.col("sk_produto").isNull() |
    F.col("sk_pagamento").isNull() |
    F.col("sk_data_pedido").isNull()
)

valid_sales_fact_df = conformed_sales_fact_df.filter(
    F.col("sk_cliente").isNotNull() &
    F.col("sk_produto").isNotNull() &
    F.col("sk_pagamento").isNotNull() &
    F.col("sk_data_pedido").isNotNull()
)

# ======================================================
# LOG (OBSERVABILIDADE)
# ======================================================

total_records = conformed_sales_fact_df.count()
valid_records = valid_sales_fact_df.count()
invalid_records = invalid_sales_fact_df.count()

print(f"[DATA QUALITY]")
print(f"Total entrada: {total_records}")
print(f"Registros válidos: {valid_records}")
print(f"Registros rejeitados: {invalid_records}")

# ======================================================
# SAVE REJECTED RECORDS
# ======================================================

if invalid_records > 0:

    invalid_sales_fact_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(rejected_sales_fact_path)

# ======================================================
# BUSINESS METRICS
# ======================================================

enriched_sales_fact_df = valid_sales_fact_df.withColumn(
    "valor_total_item",
    (
        F.col("quantidade") * F.col("preco_unitario")
    ).cast("decimal(12,2)")
)

# ======================================================
# FACT SURROGATE KEY
# ======================================================

keyed_sales_fact_df = enriched_sales_fact_df.withColumn(
    "sk_venda",
    F.sha2(
        F.concat_ws(
            "|",
            "id_pedido",
            "id_item_pedido",
            "id_produto"
        ),
        256
    )
)

# ======================================================
# FINAL SELECT
# ======================================================

refined_sales_fact_df = keyed_sales_fact_df.select(
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
# DUPLICATE VALIDATION
# ======================================================

duplicated_sales_keys_df = (
    refined_sales_fact_df
    .groupBy("sk_venda")
    .count()
    .filter("count > 1")
)

if duplicated_sales_keys_df.count() > 0:

    print("[ERRO] Duplicidade de sk_venda detectada")

    duplicated_sales_keys_df.show()

    raise Exception("Duplicidade de chave na fato")

# ======================================================
# WRITE (UPSERT REAL)
# ======================================================

if not DeltaTable.isDeltaTable(spark, sales_fact_path):

    refined_sales_fact_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(sales_fact_path)

else:

    sales_fact_delta_table = DeltaTable.forPath(
        spark,
        sales_fact_path
    )

    sales_fact_update_set = {
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

    (
        sales_fact_delta_table.alias("t")
        .merge(
            refined_sales_fact_df.alias("s"),
            "t.sk_venda = s.sk_venda"
        )

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
            set=sales_fact_update_set
        )

        .whenNotMatchedInsertAll()
        .execute()
    )

print("[FATO_VENDAS] OK")

print(
    "[REFINED] Qtd registros após o merge:",
    spark.read.format("delta").load(sales_fact_path).count()
)

spark.stop()