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
    print(f"\n--- Обработка папки: {source_path} ---")

    try:
        df_raw = spark.read \
            .option("recursiveFileLookup", "true") \
            .option("multiline", "true") \
            .json(source_path) \
            .cache()

        valid_cols = [c for c in df_raw.columns if c != "_corrupt_record"]
        if not valid_cols:
            print(f"⚠️  В папке {source_path} нет валидных данных.")
            return
    except Exception as e:
        print(f"⚠️  Критическая ошибка чтения: {str(e)}")
        return

    count = df_raw.count()
    print(f"✅ Прочитано записей: {count}")

    # --- СЕКРЕТНЫЙ ИНГРЕДИЕНТ: DROP ПЕРЕД ЗАПИСЬЮ ---
    # Это гарантирует, что старая локация будет очищена
    spark.sql(f"DROP TABLE IF EXISTS {table_name_bronze}")

    # Сохраняем Bronze
    df_raw.write.mode("overwrite").saveAsTable(table_name_bronze)
    print(f"✅ Слой Bronze готов: {table_name_bronze}")

    # Очистка для Silver
    clean_udf = F.udf(clean_text, StringType())
    target_col = next((c for c in text_col_candidates if c in df_raw.columns), None)

    if target_col:
        print(f"ℹ️  Очистка текстовой колонки: {target_col}")
        df_silver = df_raw.withColumn("clean_text", clean_udf(F.col(target_col))) \
            .filter(F.length(F.col("clean_text")) > 2)

        # Тоже удаляем перед записью
        spark.sql(f"DROP TABLE IF EXISTS {table_name_silver}")
        df_silver.write.mode("overwrite").saveAsTable(table_name_silver)
        print(f"✅ Слой Silver готов: {table_name_silver}")
    else:
        print(f"❌ Текстовая колонка не найдена. Список колонок: {df_raw.columns}")


def run_etl():
    spark = get_spark_session("ETL_Final_Fix")

    # Обрабатываем Посты
    process_dataset(spark, "/data/raw/posts", "bronze_posts", "silver_posts", ["message", "content", "text"])

    # Обрабатываем Комментарии
    process_dataset(spark, "/data/raw/comments", "bronze_comments", "silver_comments", ["message", "text", "comment"])

    spark.stop()


if __name__ == "__main__":
    run_etl()