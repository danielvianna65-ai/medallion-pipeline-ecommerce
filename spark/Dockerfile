FROM apache/spark:3.5.8

USER root

# cria diretório
RUN mkdir -p /opt/spark/external-jars

# copia jars do projeto
COPY external-jars/*.jar /opt/spark/external-jars/

# instala o delta spark
RUN pip install delta-spark==3.2.0

ENV PYTHONPATH=/opt/spark

USER spark

