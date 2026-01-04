# 1. Базовый образ
FROM python:3.10-slim-bookworm

# Устанавливаем GCC (убрали лишний слэш в конце)
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Устанавливаем Java и утилиты
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

# 🔥 ВАЖНОЕ ИСПРАВЛЕНИЕ:
# Создаем универсальную ссылку (symlink), которая будет работать и на Intel, и на Mac M1/M2.
# dpkg --print-architecture вернет 'arm64' на маке или 'amd64' на винде/интел-маке.
RUN ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/my-java-home

# Теперь JAVA_HOME указывает на нашу созданную ссылку
ENV JAVA_HOME=/usr/lib/jvm/my-java-home
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# 4. Скачиваем Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xhf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# 5. Копируем requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Рабочая директория
WORKDIR /app

CMD ["tail", "-f", "/dev/null"]