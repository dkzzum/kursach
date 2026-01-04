from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Настройки Spark (используем внутренний склад, так как мы на Плане Б)
spark = SparkSession.builder \
    .appName("Telegram_SilverToGold_Generator") \
    .config("spark.sql.warehouse.dir", "file:///opt/spark/work-dir/spark-warehouse") \
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def generate_gold_table(source_table, target_table, multiplier):
    print(f"\n--- 🔨 Генерация Gold таблицы: {target_table} (x{multiplier}) ---")

    # 1. Читаем Silver
    try:
        df_source = spark.table(source_table)
        base_count = df_source.count()
        print(f"📉 Исходных записей ({source_table}): {base_count}")
    except Exception:
        print(f"❌ Таблица {source_table} не найдена.")
        return

    # 2. Магия размножения (CrossJoin)
    # Создаем DataFrame с числами от 0 до multiplier
    df_mult = spark.range(multiplier).withColumnRenamed("id", "copy_id")

    # Умножаем данные
    df_large = df_source.crossJoin(df_mult)

    # 3. Уникализация (чтобы данные выглядели разными)
    # Сдвигаем дату назад на случайное кол-во дней (до 3 лет)
    gold_df = df_large \
        .withColumn("rand_days", (F.rand() * 1000).cast("int")) \
        .withColumn("date_ts", F.expr("date_sub(date_ts, rand_days)")) \
        .withColumn("is_synthetic", F.lit(True)) \
        .drop("copy_id", "rand_days")

    # 4. Сохраняем в Gold
    print(f"💾 Сохраняем в {target_table}...")
    gold_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(target_table)

    final_count = spark.table(target_table).count()
    print(f"✅ Готово! В {target_table} теперь {final_count} записей.")


if __name__ == "__main__":
    # У нас 650к записей.
    # Умножим на 5, чтобы получить ~3.2 миллиона.
    generate_gold_table("silver_posts", "gold_posts", multiplier=5)
    generate_gold_table("silver_comments", "gold_comments", multiplier=5)

    spark.stop()