from pendulum import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner": "DlV",
    "depends_on_past": False,
    "retries": 1,
}

TABELAS_ECOMMERCE = [
    "categorias",
    "clientes",
    "enderecos",
    "estoque",
    "itens_pedido",
    "pagamentos",
    "pedidos",
    "produtos",
]

with DAG(
    dag_id="landing_ecommerce_ingestion",
    description="Landing layer - ingestão completa do banco ecommerce via Spark",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    max_active_tasks=4,
    tags=["ecommerce", "landing", "spark"],
) as dag:

    for table in TABELAS_ECOMMERCE:
        SparkSubmitOperator(
            task_id=f"landing_{table}",
            application="/opt/spark/jobs/landing/ecommerce_landing.py",
            conn_id="spark_standalone",
            deploy_mode="client",
            application_args=[
                "--table", table,
                "--execution_date", "{{ ds }}"
            ],
            conf={
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
                "spark.sql.shuffle.partitions": "4",
                "spark.jars": "/opt/spark/external-jars/mysql-connector-j-8.3.0.jar",
            },
            verbose=True,
        )
