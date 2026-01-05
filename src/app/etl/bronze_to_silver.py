from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import re
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session


def clean_text(text):
    if text is None: return ""
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'[^а-яА-Яa-zA-Z0-9\s]', '', text)
    return text.lower().strip()


def process_dataset(spark, source_path, table_name_bronze, table_name_silver, text_col_candidates):
    """
    Универсальная функция для обработки данных (Посты или Комментарии).
    Загружает JSON, сохраняет в Bronze таблицу, очищает текст и сохраняет в Silver.
    """
    print(f"\n--- Обработка папки: {source_path} ---")

    try:
        # 1. Чтение данных
        # multiline - для красивых JSON с отступами
        # recursiveFileLookup - чтобы собрать файлы из всех подпапок date=...
        df_raw = spark.read \
            .option("recursiveFileLookup", "true") \
            .option("multiline", "true") \
            .json(source_path) \
            .cache()

        # Проверка на наличие колонок (чтобы не упасть на пустых файлах)
        valid_cols = [c for c in df_raw.columns if c != "_corrupt_record"]
        if not valid_cols:
            print(f"⚠️  В папке {source_path} не найдено валидных JSON данных.")
            return

    except Exception as e:
        print(f"⚠️  Критическая ошибка при чтении: {str(e)}")
        return

    count = df_raw.count()
    print(f"✅ Успешно прочитано записей: {count}")

    # --- ЗАПИСЬ СЛОЯ BRONZE ---
    # Сначала удаляем старые метаданные из Hive
    spark.sql(f"DROP TABLE IF EXISTS {table_name_bronze}")

    # Записываем данные. Использование option("path", ...) делает таблицу External,
    # что позволяет избежать ошибок блокировки локации.
    df_raw.write.mode("overwrite") \
        .option("path", f"/user/hive/warehouse/{table_name_bronze}") \
        .saveAsTable(table_name_bronze)
    print(f"✅ Таблица Hive '{table_name_bronze}' (Bronze) создана")

    # --- ПОДГОТОВКА СЛОЯ SILVER (ОЧИСТКА) ---
    clean_udf = F.udf(clean_text, StringType())

    # Ищем, как называется колонка с текстом в этом наборе данных
    target_col = next((c for c in text_col_candidates if c in df_raw.columns), None)

    if target_col:
        print(f"ℹ️  Очистка текста в колонке '{target_col}'...")

        # Создаем новую колонку clean_text и фильтруем пустые/короткие сообщения
        df_silver = df_raw.withColumn("clean_text", clean_udf(F.col(target_col))) \
            .filter(F.length(F.col("clean_text")) > 2)

        # Удаляем старые метаданные Silver
        spark.sql(f"DROP TABLE IF EXISTS {table_name_silver}")

        # Записываем очищенные данные в Silver таблицу
        df_silver.write.mode("overwrite") \
            .option("path", f"/user/hive/warehouse/{table_name_silver}") \
            .saveAsTable(table_name_silver)
        print(f"✅ Таблица Hive '{table_name_silver}' (Silver) создана")
    else:
        print(f"❌ Текстовая колонка не найдена. Доступные колонки: {df_raw.columns}")


def run_etl():
    spark = get_spark_session("ETL_Final_Fix")

    # Обрабатываем Посты
    process_dataset(spark, "/data/raw/posts", "bronze_posts", "silver_posts", ["message", "content", "text"])

    # Обрабатываем Комментарии
    process_dataset(spark, "/data/raw/comments", "bronze_comments", "silver_comments", ["message", "text", "comment"])

    spark.stop()


if __name__ == "__main__":
    run_etl()