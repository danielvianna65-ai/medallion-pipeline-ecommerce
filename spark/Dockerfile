FROM apache/spark:3.5.8

USER root

RUN apt-get update && \
    apt-get install -y zip unzip && \
    rm -rf /var/lib/apt/lists/*

# garante que o Python enxergue o projeto
ENV PYTHONPATH=/opt/spark

USER spark
