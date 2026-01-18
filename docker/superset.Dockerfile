FROM apache/superset:latest

USER root

# Устанавливаем драйверы для подключения к Hive/Spark Thrift Server
RUN pip install --no-cache-dir pyhive thrift thrift_sasl

USER superset