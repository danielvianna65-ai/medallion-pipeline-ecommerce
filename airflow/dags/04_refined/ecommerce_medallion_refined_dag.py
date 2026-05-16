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
    "dim_produto",
    "dim_cliente",
    "dim_pagamento",
]

SPARK_CONF = {
    "spark.executor.instances": "1",
    "spark.executor.memory": "3g",
    "spark.executor.cores": "2",
    "spark.cores.max": "2",
    "spark.driver.memory": "1g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "4",
    "spark.hadoop.dfs.replication": "1",
    "spark.master": "spark://spark-master:7077",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.hadoop.fs.defaultFS": "hdfs://namenode:8020",
    "spark.sql.catalogImplementation": "hive",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
    "spark.sql.warehouse.dir": "hdfs://namenode:8020/user/hive/warehouse",
    "spark.jars": (
    "/opt/spark/external-jars/delta-spark_2.12-3.2.0.jar,"
    "/opt/spark/external-jars/delta-storage-3.2.0.jar,"
    "/opt/spark/external-jars/postgresql-42.7.3.jar"
)

}

SPARK_CONF_FATO = {
    "spark.executor.instances": "1",
    "spark.executor.memory": "14g",
    "spark.executor.cores": "8",
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "16",
    "spark.hadoop.dfs.replication": "1",
    "spark.sql.adaptive.enabled": "true",
    "spark.master": "spark://spark-master:7077",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.hadoop.fs.defaultFS": "hdfs://namenode:8020",
    "spark.sql.catalogImplementation": "hive",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
    "spark.sql.warehouse.dir": "hdfs://namenode:8020/user/hive/warehouse",
    "spark.jars": (
    "/opt/spark/external-jars/delta-spark_2.12-3.2.0.jar,"
    "/opt/spark/external-jars/delta-storage-3.2.0.jar,"
    "/opt/spark/external-jars/postgresql-42.7.3.jar"
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
                name=f"refined-{table}",
                conf=SPARK_CONF,
                verbose=True,
            )

    # ==================================================
    # TASK GROUP - FATO
    # ==================================================

    with TaskGroup(group_id="fato", tooltip="Construção da tabela fato") as fato_group:

        fato_vendas = SparkSubmitOperator(
            task_id="fato_vendas",
            application="/opt/spark/jobs/04_refined/refined_fato_vendas.py",
            conn_id="spark_standalone",
            deploy_mode="client",
            name="refined-fato-vendas",
            conf=SPARK_CONF_FATO,
        )

    # ==================================================
    # DEPENDÊNCIA ENTRE GRUPOS
    # ==================================================

    dim_group >> fato_group