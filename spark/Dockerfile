FROM apache/spark:3.5.8

USER root

RUN apt-get update && \
    apt-get install -y zip unzip && \
    rm -rf /var/lib/apt/lists/*

# cria diretório
RUN mkdir -p /opt/spark/external-jars

# copia jars do projeto
COPY external-jars/*.jar /opt/spark/external-jars/

# instala o delta spark
RUN pip install delta-spark==3.2.0

ENV PYTHONPATH=/opt/spark

USER spark

