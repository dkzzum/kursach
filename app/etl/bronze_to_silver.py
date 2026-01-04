from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, regexp_replace, trim
import os

# 1. Инициализация Spark
# Мы явно указываем Derby создавать служебные файлы в /tmp,
# чтобы избежать проблем с блокировками на Mac.
spark = SparkSession.builder \
    .appName("TelegramETL_BronzeToSilver") \
    .config("spark.sql.warehouse.dir", "file:///opt/spark/work-dir/spark-warehouse") \
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def process_posts():
    print("--- 🚀 Начало обработки POSTS (Bronze -> Silver) ---")

    # 1. Читаем сырые JSON (Bronze Layer)
    raw_posts_path = "/data/raw/posts"
    try:
        df = spark.read.option("multiLine", True).json(raw_posts_path)
    except Exception as e:
        print(f"⚠️ Ошибка чтения постов (возможно папка пустая): {e}")
        return

    print(f"📥 Загружено сырых записей: {df.count()}")

    # 2. Очистка данных (Transformation)
    cleaned_df = df \
        .dropDuplicates(['post_id', 'channel_id']) \
        .withColumn("date_ts", to_timestamp(col("date"), "yyyy-MM-dd")) \
        .withColumn("clean_text", regexp_replace(col("text"), "<[^>]+>", "")) \
        .withColumn("clean_text", trim(col("clean_text"))) \
        .filter(col("clean_text") != "")  # Убираем пустые посты

    # Примечание: regexp_replace("<[^>]+>", "") удаляет HTML теги (<b>, <br> и т.д.)

    print(f"✨ Записей после очистки: {cleaned_df.count()}")

    # 3. Сохранение в Hive (Silver Layer)
    # format("parquet") — сохраняем в эффективном формате
    # saveAsTable — регистрирует таблицу в Hive metastore
    table_name = "silver_posts"
    cleaned_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(table_name)

    print(f"💾 Таблица '{table_name}' успешно сохранена в Hive!")
    print("-" * 30)


def process_comments():
    print("--- 🚀 Начало обработки COMMENTS (Bronze -> Silver) ---")

    raw_comments_path = "/data/raw/comments"
    try:
        df = spark.read.option("multiLine", True).json(raw_comments_path)
    except Exception:
        print("⚠️ Комментарии не найдены.")
        return

    print(f"📥 Загружено сырых комментариев: {df.count()}")

    cleaned_df = df \
        .dropDuplicates(['comment_id']) \
        .withColumn("date_ts", to_timestamp(col("date"), "yyyy-MM-dd")) \
        .withColumn("clean_text", regexp_replace(col("text"), "<[^>]+>", "")) \
        .withColumn("clean_text", trim(col("clean_text")))

    table_name = "silver_comments"
    cleaned_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(table_name)

    print(f"💾 Таблица '{table_name}' успешно сохранена в Hive!")


if __name__ == "__main__":
    process_posts()
    process_comments()
    spark.stop()

# Запуск: docker exec -it spark_processor python etl_bronze_to_silver.py
