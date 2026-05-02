import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta

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
    dag_id="02_raw_delta_merge_ecommerce",
    description="Raw layer marge - processamento incremental via Spark com Delta Lake (MERGE INTO).",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
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
            name=f"raw-ecommerce-{table}",
            # ==========================
            # Configurações Spark / Delta
            # ==========================
            conf={
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
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
    # ===============================================
    # Trigger próxima DAG (RAW)
    # ===============================================
    trigger_trusted = TriggerDagRunOperator(
        task_id="trigger_trusted_ecommerce",
        trigger_dag_id="03_trusted_ecommerce",
        wait_for_completion=False
    )

    # Todas as tabelas precisam terminar
    raw_tasks >> trigger_trusted