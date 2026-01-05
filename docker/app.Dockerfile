FROM apache/spark:3.5.0

USER root

# 1. Обновляем списки пакетов и устанавливаем компиляторы
RUN apt-get update && \
    apt-get install -y gcc python3-dev build-essential && \
    apt-get clean

# 2. Обновляем сам pip
RUN pip install --upgrade pip setuptools wheel

# 3. Копируем и устанавливаем зависимости
# ИЗМЕНЕНИЕ: Путь теперь config/requirements.txt, так как контекст сборки - корень проекта
COPY config/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 4. Скачиваем драйвер Postgres (для Hive)
RUN curl -o /opt/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# ИЗМЕНЕНИЕ: Новая рабочая директория
WORKDIR /opt/application

# ИЗМЕНЕНИЕ: Настраиваем переменную окружения для Python, чтобы он видел папку src
ENV PYTHONPATH="${PYTHONPATH}:/opt/application/src"

# Возвращаемся к пользователю spark (если нужно, но в compose у вас стоит user: root, так что это опционально)
USER 185