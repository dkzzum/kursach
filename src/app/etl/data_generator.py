import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# Добавляем путь к корню для импорта конфига
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session


def generate_gold_table(spark, source_table, target_table, multiplier):
    print(f"\n--- 🔨 Генерация {target_table} (Умножение x{multiplier}) ---")

    # 1. Читаем исходную Silver-таблицу
    try:
        df_source = spark.read.table(source_table)
        base_count = df_source.count()
        print(f"📉 Исходных записей ({source_table}): {base_count}")

        if base_count == 0:
            print("⚠️ Исходная таблица пуста, пропускаем.")
            return

    except Exception as e:
        print(f"❌ Ошибка чтения {source_table}: {e}")
        return

    # 2. Магия размножения (CrossJoin)
    # Создаем маленький датафрейм с числами от 0 до multiplier
    # Это позволит размножить каждую строку в multiplier раз
    df_mult = spark.range(multiplier).withColumnRenamed("id", "copy_id")

    # Умножаем данные
    df_large = df_source.crossJoin(df_mult)

    # 3. Синтезация (делаем данные "уникальными")
    # - Сдвигаем дату назад на случайное число дней (до 3 лет / 1000 дней)
    # - Добавляем пометку is_synthetic

    # Определяем колонку с датой (в silver это 'date')
    date_col = "date" if "date" in df_source.columns else None

    if date_col:
        print("🔄 Генерация смещения дат...")
        gold_df = df_large \
            .withColumn("rand_days", (F.rand() * 1000).cast("int")) \
            .withColumn("date", F.date_sub(F.col(date_col).cast(DateType()), F.col("rand_days"))) \
            .withColumn("is_synthetic", F.lit(True)) \
            .drop("copy_id", "rand_days")
    else:
        print("⚠️ Колонка даты не найдена, просто дублируем данные.")
        gold_df = df_large.withColumn("is_synthetic", F.lit(True)).drop("copy_id")

    # 4. Сохраняем в Gold (Hive External Table)
    # Используем нашу безопасную логику с удалением и путем
    print(f"💾 Сохранение в Hive таблицу {target_table}...")

    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    gold_df.write \
        .mode("overwrite") \
        .option("path", f"/user/hive/warehouse/{target_table}") \
        .saveAsTable(target_table)

    final_count = spark.read.table(target_table).count()
    print(f"✅ Готово! В {target_table} теперь {final_count} записей.")


def run_generation():
    spark = get_spark_session("Data_Generator_Synthesizer")
    spark.sparkContext.setLogLevel("WARN")

    # У тебя сейчас ~12k записей.
    # Чтобы получить "Big Data" (хотя бы 2.5 - 3 млн строк), нужно умножить на ~200-250.
    # Для курсовой это будет выглядеть солидно.

    # 1. Генерируем Посты (для графика активности)
    generate_gold_table(spark, "silver_posts", "gold_posts", multiplier=5)

    # 2. Генерируем Комментарии (для топа авторов)
    # Здесь можно меньше множитель, если не хочешь ждать слишком долго,
    # но для "Big Data ML" лучше тоже побольше.
    generate_gold_table(spark, "silver_comments", "gold_comments", multiplier=5)

    spark.stop()


if __name__ == "__main__":
    run_generation()