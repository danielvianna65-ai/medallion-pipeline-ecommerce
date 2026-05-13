FROM apache/superset:latest

USER root

RUN /app/.venv/bin/python -m ensurepip && \
    /app/.venv/bin/python -m pip install --upgrade pip && \
    /app/.venv/bin/python -m pip install \
    pyhive \
    jaydebeapi \
    JPype1 \
    thrift==0.16.0 \
    pure-sasl==0.6.2 \
    thrift-sasl==0.4.3

USER superset