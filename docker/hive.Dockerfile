FROM apache/hive:4.0.0

USER root

# 1. Обновляем репозитории и устанавливаем curl (в образе hive его нет)
RUN apt-get update && \
    apt-get install -y curl

# 2. Скачиваем драйвер прямо в папку библиотек Hive
RUN curl -L -o /opt/hive/lib/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# 3. Возвращаем права пользователю hive
USER hive