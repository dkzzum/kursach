FROM apache/spark:3.5.0

USER root

# 1. Обновляем списки пакетов и устанавливаем компиляторы (нужны для сборки библиотек на M1/M2)
RUN apt-get update && \
    apt-get install -y gcc python3-dev build-essential && \
    apt-get clean

# 2. Обновляем сам pip, setuptools и wheel (важно для новых библиотек)
RUN pip install --upgrade pip setuptools wheel

# 3. Копируем и устанавливаем зависимости
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 4. Скачиваем драйвер Postgres (для Hive)
RUN curl -o /opt/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

WORKDIR /app

# Возвращаемся к пользователю spark
USER 185