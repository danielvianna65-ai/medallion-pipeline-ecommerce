import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup


# ======================================================
# CONFIG
# ======================================================

DIM_JOBS = [
    "dim_data",
    "dim_categoria",
    "dim_produto",
    "dim_cliente",
    "dim_pagamento",
]

SPARK_CONF = {
    "spark.master": "spark://spark-master:7077",
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
}

DEFAULT_ARGS = {
    "owner": "DlV",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# ======================================================
# DAG
# ======================================================

with DAG(
    dag_id="04_ecommerce_refined",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["refined", "dw", "ecommerce"],
) as dag:

    # ==================================================
    # TASK GROUP - DIMENSÕES
    # ==================================================

    with TaskGroup(group_id="dimensoes", tooltip="Construção das dimensões") as dim_group:

        dim_tasks = {}

        for table in DIM_JOBS:
            dim_tasks[table] = SparkSubmitOperator(
                task_id=table,
                application=f"/opt/spark/jobs/04_refined/refined_{table}.py",
                conn_id="spark_standalone",
                deploy_mode="client",
                py_files="/opt/spark/app/common.zip",
                name=f"refined-{table}",
                conf=SPARK_CONF,
                verbose=True,
            )

        ordered_dims = [
            "dim_data",
            "dim_categoria",
            "dim_produto",
            "dim_cliente",
            "dim_pagamento",
        ]

        for i in range(len(ordered_dims) - 1):
            dim_tasks[ordered_dims[i]] >> dim_tasks[ordered_dims[i + 1]]

    # ==================================================
    # TASK GROUP - FATO
    # ==================================================

    with TaskGroup(group_id="fato", tooltip="Construção da tabela fato") as fato_group:

        fato_vendas = SparkSubmitOperator(
            task_id="fato_vendas",
            application="/opt/spark/jobs/04_refined/refined_fato_vendas.py",
            conn_id="spark_standalone",
            deploy_mode="client",
            py_files="/opt/spark/app/common.zip",
            name="refined-fato-vendas",
            conf=SPARK_CONF,
        )

    # ==================================================
    # DEPENDÊNCIA ENTRE GRUPOS
    # ==================================================

    dim_group >> fato_group