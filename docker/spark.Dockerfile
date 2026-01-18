# Переходим на официальный образ Apache Spark (он всегда доступен)
FROM apache/spark:3.5.0

# Переключаемся на root для установки библиотек
USER root

# Устанавливаем pip (на всякий случай) и библиотеки numpy/pandas
# Образ apache/spark основан на Ubuntu/Debian, поэтому используем apt-get если pip нет,
# но обычно python3 там уже стоит.
RUN pip install --no-cache-dir numpy pandas

ADD https://jdbc.postgresql.org/download/postgresql-42.6.0.jar /opt/spark/jars/postgresql-42.6.0.jar

# Возвращаем права стандартному пользователю Spark (UID 185)
USER 185