import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta


TRUSTED_JOBS = [
    "clientes",
    "categorias",
    "produtos",
    "pedidos",
    "itens_pedido",
    "estoque",
    "pagamentos",
    "enderecos",
]

DEFAULT_ARGS = {
    "owner": "DlV",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="03_trusted_ecommerce",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    description="Pipeline Trusted  Layer - Limpeza, padronização e inrequicimento",
    schedule_interval=None,
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    default_args=DEFAULT_ARGS ,
    tags=["ecommerce", "trusted", "spark"],
) as dag:

    trusted_tasks = []

    for tabela in TRUSTED_JOBS:
        task = SparkSubmitOperator(
            task_id=f"trusted_{tabela}",
            application=f"/opt/spark/jobs/03_trusted/trusted_{tabela}.py",
            conn_id="spark_standalone",
            deploy_mode="client",
            py_files="/opt/spark/app/common.zip",
            name=f"trusted-ecommerce-{tabela}",
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

        trusted_tasks.append(task)
    # ===============================================
    # Trigger próxima DAG (Refined)
    # ===============================================
    trigger_refined = TriggerDagRunOperator(
        task_id="trigger_refined_ecommerce",
        trigger_dag_id="04_ecommerce_refined",
        wait_for_completion=False
    )

    # Todas as tabelas precisam terminar
    trusted_tasks >> trigger_refined