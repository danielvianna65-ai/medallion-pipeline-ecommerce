# ======================================================
# IMPORTS
# ======================================================
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    dag_id="landing_clientes_enrichment",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    tags=["clientes_enrichment", "landing", "spark", "delta"],
) as dag:

    landing_clientes_enrichment = SparkSubmitOperator(
        task_id="landing_clientes_enrichment",
        application="/opt/spark/jobs/01_landing/landing_clientes_enrichment.py",
        conn_id="spark_standalone",
        deploy_mode="client",
        name="landing_clientes_enrichment",
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.driver.memory": "1g",
            "spark.hadoop.dfs.replication": "1",
        },
        verbose=True,
    )

    # ===============================================
    # Trigger próxima DAG (RAW)
    # ===============================================
    trigger_raw = TriggerDagRunOperator(
        task_id="trigger_raw_enrichment_layer",
        trigger_dag_id="raw_clientes_enrichment",
        wait_for_completion=False
    )

    # ===============================================
    # Ordem de execução
    # ===============================================
    landing_clientes_enrichment >> trigger_raw