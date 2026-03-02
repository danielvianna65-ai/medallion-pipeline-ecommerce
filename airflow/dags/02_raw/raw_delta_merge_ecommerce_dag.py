from pendulum import datetime, timezone
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta

# =========================================================
# Timezone local
# =========================================================
LOCAL_TZ = timezone("America/Sao_Paulo")

# =========================================================
# Tabelas do domínio ecommerce
# =========================================================
TABLES_ECOMMERCE = [
    "pagamentos",
    "pedidos",
    "clientes",
    "produtos",
    "itens_pedido",
    "categorias",
    "estoque",
    "enderecos",
]

# =========================================================
# Default args
# =========================================================
DEFAULT_ARGS = {
    "owner": "DlV",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# =========================================================
# DAG
# =========================================================
with DAG(
    dag_id="raw_delta_merge_ecommerce",
    description="Raw layer marge - processamento incremental via Spark com Delta Lake (MERGE INTO).",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    tags=["ecommerce", "raw", "delta", "merge", "spark"],
) as dag:

    raw_tasks = []

    # =====================================================
    # Loop de criação das tasks (1 job por tabela)
    # =====================================================
    for table in TABLES_ECOMMERCE:

        task = SparkSubmitOperator(
            task_id=f"raw_{table}",
            application=f"/opt/spark/jobs/02_raw/raw_{table}.py",
            conn_id="spark_standalone",
            deploy_mode="client",
            py_files="/opt/spark/app/common.zip",
            name=f"raw-ecommerce-{table}",

            # ==========================
            # Args para o job Spark
            # ==========================
            application_args=[
                "--execution_date",
                "{{ data_interval_start.in_timezone('America/Sao_Paulo').to_date_string() }}",
                "--landing_base",
                "hdfs://namenode:8020/data/01_landing/ecommerce",
                "--raw_base",
                "hdfs://namenode:8020/data/02_raw/ecommerce",
            ],

            # ==========================
            # Configurações Spark / Delta
            # ==========================
            conf={
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "1g",
                "spark.sql.shuffle.partitions": "8",
                "spark.hadoop.dfs.replication": "1",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.jars": (
                    "/opt/spark/external-jars/delta-spark_2.12-3.2.0.jar,"
                    "/opt/spark/external-jars/delta-storage-3.2.0.jar"
                )
            },

            verbose=True,
        )

        raw_tasks.append(task)
