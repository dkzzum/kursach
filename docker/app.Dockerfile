# 1. Используем стабильный Debian 12 (Bookworm), где точно есть Java 17
FROM python:3.8-slim-bookworm

WORKDIR /app

# 2. Устанавливаем OpenJDK 17 и procps (нужен для Spark)
# gcc нужен для сборки некоторых python-библиотек
RUN apt-get update && \
    apt-get install -y gcc openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

# 3. Задаем JAVA_HOME (стандартный путь для Debian)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# 4. Устанавливаем зависимости
COPY config/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Копируем код
COPY src ./src

# 6. Настраиваем окружение
ENV PYTHONPATH=/app/src

# 7. Держим контейнер активным
CMD ["tail", "-f", "/dev/null"]