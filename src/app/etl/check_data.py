import os
import sys
from pyspark.sql import functions as F

# Добавляем путь к корню для импорта конфига
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session


def check_table(spark, table_name):
    print(f"\n" + "=" * 60)
    print(f"🔎 ПРОВЕРКА ТАБЛИЦЫ: {table_name}")
    print("=" * 60)

    # Проверяем, существует ли таблица в Hive
    if not spark.catalog.tableExists(table_name):
        print(f"❌ Таблица '{table_name}' не найдена в Hive.")
        return

    try:
        df = spark.read.table(table_name)

        # 1. Количество строк
        count = df.count()
        print(f"📊 Всего записей: {count}")

        if count == 0:
            print("⚠️ Таблица пуста.")
            return

        # 2. Определение колонок
        cols = df.columns
        print(f"📋 Колонки: {', '.join(cols)}")

        # 3. Проверка диапазона дат
        # Пытаемся найти колонку с датой (в разных таблицах она может называться по-разному)
        date_col = None
        if "date" in cols:
            date_col = "date"
        elif "date_ts" in cols:
            date_col = "date_ts"

        if date_col:
            print(f"📅 Диапазон дат (по колонке '{date_col}'):")
            df.select(F.min(date_col), F.max(date_col)).show(truncate=False)
        else:
            print("ℹ️ Колонка даты не найдена.")

        # 4. Примеры данных
        # Ищем текстовую колонку для превью
        text_col = "clean_text" if "clean_text" in cols else ("text" if "text" in cols else None)

        if text_col:
            print(f"📝 Примеры (из колонки '{text_col}'):")
            # Если есть дата, выводим с датой, иначе просто текст
            if date_col:
                df.select(date_col, F.substring(F.col(text_col), 1, 60).alias("preview")).limit(5).show(truncate=False)
            else:
                df.select(F.substring(F.col(text_col), 1, 80).alias("preview")).limit(5).show(truncate=False)

        # 5. Если это таблица с результатами ML, покажем статистику
        if "prediction" in cols:
            print("🧠 Статистика классификации (0.0=Neutral, 1.0=Toxic):")
            df.groupBy("prediction").count().show()

    except Exception as e:
        print(f"❌ Ошибка при чтении таблицы: {e}")


def run_check():
    # Инициализация с правильным конфигом Hive
    spark = get_spark_session("Data_Inspector_Tool")
    spark.sparkContext.setLogLevel("WARN")

    # Список таблиц для проверки (пройдемся по всем слоям)
    tables_to_check = [
        "bronze_posts",
        "silver_posts",
        "silver_comments",
        "gold_posts",  # Синтетика
        "gold_comments",  # Синтетика
        "platinum_destructive",  # Словарь
        "gold_bigdata_predictions"  # Финальный ML
    ]

    for table in tables_to_check:
        check_table(spark, table)

    spark.stop()
    print("\n✅ Диагностика завершена.")


if __name__ == "__main__":
    run_check()