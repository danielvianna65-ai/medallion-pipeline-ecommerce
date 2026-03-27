# =====================================================
# IMPORTS
# =====================================================
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta

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
    dag_id="trusted_clientes_enrichment",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    tags=["clientes_enrichment", "trusted", "spark", "delta"],
) as dag:

    trusted_clientes_enrichment = SparkSubmitOperator(
        task_id="trusted_clientes_enrichment",
        application="/opt/spark/jobs/03_trusted/trusted_clientes_enrichment.py",
        conn_id="spark_standalone",
        deploy_mode="client",
        name="trusted_clientes_enrichment",
        packages="io.delta:delta-spark_2.12:3.2.0",
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.driver.memory": "1g",
            "spark.sql.shuffle.partitio": "8",
            "spark.hadoop.dfs.replication": "1",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        verbose=True,
    )
