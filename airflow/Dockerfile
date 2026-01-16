FROM apache/airflow:2.10.5-python3.8

USER root

# Java 17 (compatível com Spark 3.4.x e 3.5.x)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Provider Spark (NÃO instala Spark)
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.11.0
