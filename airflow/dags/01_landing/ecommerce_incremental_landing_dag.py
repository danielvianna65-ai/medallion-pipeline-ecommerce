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
    "categorias",
    "clientes",
    "enderecos",
    "estoque",
    "itens_pedido",
    "pagamentos",
    "pedidos",
    "produtos",
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
    dag_id="landing_incremental_ecommerce",
    description="Landing layer incremental - ingestão universal via watermark",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    tags=["ecommerce", "landing", "incremental", "spark"],
) as dag:

    raw_tasks = []

    # =====================================================
    # Loop de criação das tasks (1 job por tabela)
    # =====================================================
    for table in TABLES_ECOMMERCE:

        task = SparkSubmitOperator(
            task_id=f"landing_{table}",
            application="/opt/spark/jobs/01_landing/landing_incremental.py",
            conn_id="spark_standalone",
            deploy_mode="client",
            name=f"landing-ecommerce-{table}",

            # ==========================
            # Args para o job Spark
            # ==========================
            application_args=[
                "--table", table,
                "--landing_base", "hdfs://namenode:8020/data/01_landing/ecommerce",
                "--jdbc_url", "jdbc:mysql://172.17.0.1:3306/ecommerce?useSSL=false&allowPublicKeyRetrieval=true",
                "--jdbc_user", "spark",
                "--jdbc_password", "Spark@123#2026",
                "--execution_date",
                "{{ data_interval_start.in_timezone('America/Sao_Paulo').to_date_string() }}"
            ],
            # ==========================
            # Configurações Spark / MySQL
            # ==========================
            conf={
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "1g",
                "spark.sql.shuffle.partitions": "8",
                "spark.hadoop.dfs.replication": "1",
                "spark.jars": "/opt/spark/external-jars/mysql-connector-j-8.3.0.jar",
            },
            verbose=True,
        )

        raw_tasks.append(task)