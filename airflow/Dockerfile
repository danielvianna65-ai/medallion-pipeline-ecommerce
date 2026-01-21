FROM apache/airflow:2.10.5-python3.10

USER root

# -----------------------------
# Java (compatível com Spark 3.5.x)
# -----------------------------
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# -----------------------------
# Airflow Spark Provider
# -----------------------------
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.11.0

# -----------------------------
# PySpark (VERSÃO EXATA DO CLUSTER)
# -----------------------------
RUN pip install --no-cache-dir pyspark==3.5.8
