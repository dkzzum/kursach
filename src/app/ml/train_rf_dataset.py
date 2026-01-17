import os
import shutil
import sys
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# Добавляем путь к корню для импорта конфига
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session


def main():
    print("🚀 Запуск обучения Random Forest и анализа Hive таблиц...")

    # 1. Инициализация Spark
    spark = get_spark_session("ToxicCommentRF_HiveAnalysis")
    spark.sparkContext.setLogLevel("WARN")

    # ==========================================
    # ЭТАП 1: Обучение модели на dataset.csv
    # ==========================================

    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_path = os.path.join(current_dir, "dataset.csv")

    print(f"⏳ Загрузка обучающих данных из: {dataset_path}")

    # Читаем датасет
    df = spark.read.csv(dataset_path, header=True, inferSchema=True)

    # Очистка и подготовка обучающей выборки
    train_data = df.filter(col("text").isNotNull()) \
        .withColumn("label", col("is_destructive").cast("double")) \
        .select("text", "label")

    print(f"📊 Размер обучающей выборки: {train_data.count()} строк")

    # Создаем пайплайн
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20)

    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, rf])

    print("🏋️‍♂️ Начало обучения модели...")
    model = pipeline.fit(train_data)
    print("✅ Модель обучена!")

    # ==========================================
    # ЭТАП 2: Предсказание на данных из Hive
    # ==========================================

    table_source = "silver_comments"
    table_target = "gold_random_forest_predictions"

    print(f"🔍 Чтение данных из таблицы Hive: {table_source}...")

    try:
        # Читаем Silver слой
        silver_df = spark.read.table(table_source)

        # Подготовка данных для модели:
        # 1. Берем 'clean_text' и называем его 'text' (так ждет модель)
        # 2. Сохраняем 'author_name' и 'clean_text' для итогового отчета
        input_df = silver_df.select(
            col("author_name"),
            col("clean_text").alias("original_content"),  # Сохраним контент под понятным именем
            col("clean_text").alias("text")  # Копия для модели
        ).filter(col("text").isNotNull())

        print("🔮 Запуск предсказания...")
        predictions = model.transform(input_df)

        # ==========================================
        # ЭТАП 3: Формирование Gold таблицы
        # ==========================================

        # Функция для извлечения вероятности токсичности (число от 0 до 1)
        extract_prob_udf = udf(lambda v: float(v[1]), FloatType())

        # ВАЖНО: Здесь мы формируем финальную структуру и даем имена,
        # которые потом используем в фильтрах и отчетах
        final_df = predictions.select(
            col("author_name"),
            col("original_content").alias("comment_text"),
            col("prediction").alias("is_toxic_pred"),  # <--- Вот имя колонки
            extract_prob_udf(col("probability")).alias("toxicity_score")
        )

        print(f"💾 Сохранение результатов в таблицу Hive: {table_target}...")

        # Очистка метаданных Hive
        spark.sql(f"DROP TABLE IF EXISTS {table_target}")

        # Физическая очистка папки (для надежности при перезапусках)
        warehouse_dir = "/user/hive/warehouse"
        target_path = f"{warehouse_dir}/{table_target}"

        if os.path.exists(target_path):
            print(f"🧹 Очистка пути: {target_path}")
            shutil.rmtree(target_path, ignore_errors=True)

        # Создаем папку склада, если её нет
        os.makedirs(warehouse_dir, exist_ok=True)

        # Сохраняем
        final_df.write \
            .mode("overwrite") \
            .option("path", target_path) \
            .saveAsTable(table_target)

        print("✅ Успешно сохранено!")
        print("-" * 30)

        # 1. Проверяем, училась ли вообще модель на плохом
        print("📊 Проверка обучающей выборки (были ли там вообще токсичные?):")
        train_data.groupBy("label").count().show()

        # 2. Смотрим статистику уверенности модели
        print("📈 Статистика предсказанных вероятностей:")
        final_df.select("toxicity_score").describe().show()

        # 3. ВАЖНО: Смотрим топ по ВЕРОЯТНОСТИ, а не по предсказанию
        # Даже если предсказание 0, но вероятность 0.45 — это наш клиент
        print("👀 Топ-10 самых 'подозрительных' комментариев (сортировка по score):")
        final_df.sort(col("toxicity_score").desc()) \
            .select("author_name", "comment_text", "toxicity_score", "is_toxic_pred") \
            .show(10, truncate=50)

        # 4. (Опционально) Если вы видите, что score доходит до 0.3-0.4,
        # можно считать токсичным всё, что выше 0.25 (ручной порог)
        print("🔥 Сколько нашлось бы при снижении порога до 25%?:")
        count_low_threshold = final_df.filter("toxicity_score > 0.25").count()
        print(f"Найдено записей: {count_low_threshold}")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()