from pendulum import datetime, timezone
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

LOCAL_TZ = timezone("America/Sao_Paulo")

TABELAS = [
    "pagamentos",
    "pedidos",
    "clientes",
    "produtos",
    "itens_pedido",
    "categorias",
    "estoque",
    "enderecos",
]

default_args = {
    "owner": "DlV",
    "retries": 1,
}

with DAG(
    dag_id="raw_ecommerce",
    start_date=datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    default_args=default_args,
    tags=["ecommerce", "raw", "spark"],
) as dag:

    for tabela in TABELAS:
        SparkSubmitOperator(
            task_id=f"raw_{tabela}",
            application=f"/opt/spark/jobs/raw/raw_{tabela}.py",
            conn_id="spark_standalone",
            deploy_mode="client",
            py_files="/opt/spark/app/common.zip",
            name=f"raw-ecommerce-{tabela}",
            conf={
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "1g",
                "spark.sql.shuffle.partitions": "8",
            },
            verbose=True,
        )
