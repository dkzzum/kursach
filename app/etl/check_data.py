from pyspark.sql import SparkSession

# 1. Настройка Spark (Копия настроек из ETL)
spark = SparkSession.builder \
    .appName("CheckSilverData") \
    .config("spark.sql.warehouse.dir", "file:///opt/spark/work-dir/spark-warehouse") \
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def check_table(table_name):
    print(f"\n--- 🔎 Проверка таблицы: {table_name} ---")

    # Проверяем, существует ли таблица
    if not spark.catalog.tableExists(table_name):
        print(f"❌ Таблица {table_name} не найдена! Сначала запустите bronze_to_silver.py")
        return

    # 1. Количество строк
    count = spark.sql(f"SELECT count(*) FROM {table_name}").collect()[0][0]
    print(f"📊 Всего записей: {count}")

    if count == 0:
        return

    # 2. Диапазон дат (если есть колонка date_ts)
    print("📅 Диапазон дат:")
    try:
        spark.sql(f"SELECT min(date_ts), max(date_ts) FROM {table_name}").show(truncate=False)
    except Exception:
        print("   (Колонка date_ts не найдена)")

    # 3. Пример данных (Текст обрезаем до 50 символов для красоты)
    print("📝 Примеры данных (Топ-5 свежих):")
    try:
        spark.sql(f"""
            SELECT date_ts, substring(clean_text, 1, 50) as text_preview 
            FROM {table_name} 
            ORDER BY date_ts DESC 
            LIMIT 5
        """).show(truncate=False)
    except Exception as e:
        print(f"   ⚠️ Ошибка вывода примеров: {e}")


if __name__ == "__main__":
    check_table("silver_posts")
    check_table("silver_comments")

    spark.stop()
    print("\n✅ Проверка завершена.")
