import os
import sys
from pyspark.sql import functions as F

# Импорт конфига
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session

def check_hive_table(spark, table_name):
    print(f"\n" + "-" * 60)
    print(f"CHECKING HIVE TABLE: {table_name}")
    print("-" * 60)

    # 1. Проверяем наличие в каталоге Hive
    if not spark.catalog.tableExists(table_name):
        print(f"[MISSING] Table '{table_name}' NOT found in Hive Metastore.")
        return

    try:
        # Читаем как таблицу (SQL)
        df = spark.table(table_name)
        
        # 2. Количество строк
        count = df.count()
        print(f"[OK] Table exists. Total records: {count}")

        if count == 0:
            print("[WARNING] Table is empty.")
            return

        # 3. Схема (Schema)
        print("Schema:")
        df.printSchema()

        # 4. Пример данных
        # Ищем текстовую колонку для примера
        cols = df.columns
        text_col = next((c for c in ["comment_text", "original_content", "text", "clean_text"] if c in cols), None)

        if text_col:
            print(f"Sample data (column '{text_col}'):")
            df.select(F.substring(F.col(text_col), 1, 80).alias("preview")).limit(5).show(truncate=False)

    except Exception as e:
        print(f"[ERROR] Failed to read table '{table_name}': {e}")

def run_hive_check():
    print("Initializing Spark with Hive support...")
    spark = get_spark_session("Hive_Inspector_Tool")
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Выводим список всех таблиц в базе default
    print("\n" + "=" * 60)
    print("CURRENT TABLES IN HIVE (Database: default)")
    print("=" * 60)
    try:
        spark.sql("SHOW TABLES").show(truncate=False)
    except Exception as e:
        print(f"[CRITICAL ERROR] Cannot connect to Hive Metastore: {e}")
        spark.stop()
        return

    # 2. Список ожидаемых таблиц для детальной проверки
    # (Добавь или убери таблицы, которые ты хочешь проверить)
    expected_tables = [
        "silver_comments",
        "silver_posts",
        "gold_random_forest_predictions",
        "gold_synthetic_posts",
        "gold_synthetic_comments",
        
    ]

    for table in expected_tables:
        check_hive_table(spark, table)

    spark.stop()
    print("\n[SUCCESS] Hive diagnostics completed.")

if __name__ == "__main__":
    run_hive_check()