import os
import sys
import time
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Добавляем путь к корню проекта
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session

# Используем наш единый конфиг для Hive
spark = get_spark_session("ML_Supervised_Scaling_Test")
spark.sparkContext.setLogLevel("WARN")


def run_supervised_experiment(table_name='silver_comments'):
    print("\n--- 🎓 Масштабируемое обучение (Supervised Learning) ---")

    # 1. Загружаем размеченный "учебник" (CSV)
    csv_path = "/app/ml/labeled.csv"
    try:
        train_base_df = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .csv(csv_path) \
            .withColumn("label", F.col("toxic").cast(DoubleType())) \
            .withColumnRenamed("comment", "text") \
            .dropna(subset=["text"])
    except Exception as e:
        print(f"❌ Ошибка чтения CSV: {e}")
        return

    # 2. Список объемов данных для эксперимента (Scalability Test)
    fractions = [0.1, 0.5, 1.0]
    results = []

    # Обучаем модель (код без изменений)
    model = None
    last_count = 0

    for frac in fractions:
        print(f"\n🔄 Тестирование объема: {int(frac * 100)}% данных...")
        sample_df = train_base_df.sample(False, frac, seed=42)
        count = sample_df.count()

        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20000)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)
        pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])

        start_time = time.time()
        (training_data, test_data) = sample_df.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(training_data)
        duration = time.time() - start_time

        predictions = model.transform(test_data)
        evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)

        print(f"📊 Строк: {count} | Время: {duration:.2f} сек | Точность: {accuracy:.4f}")
        results.append((count, duration, accuracy))
        last_count = count

    # 3. ПРИМЕНЕНИЕ К ДАННЫМ ИЗ HIVE
    print(f"\n--- 🚀 Применение к данным из Hive ({table_name}) ---")
    try:
        # Читаем таблицу и СРАЗУ убираем конфликт имен
        hive_df = spark.read.table(table_name) \
            .withColumnRenamed("text", "original_text")

        # Теперь безопасно берем clean_text и называем его text (как ждет модель)
        prediction_input = hive_df.withColumn("text", F.col("clean_text"))

        final_predictions = model.transform(prediction_input)

        # Формируем финальную выборку без лишних колонок
        final_df = final_predictions.select(
            "author_name",
            "original_text",  # Сохраняем оригинальный текст для просмотра
            "clean_text",
            "prediction"
        )

        # Сохраняем в таблицу Gold
        output_table = "gold_supervised_predictions"
        spark.sql(f"DROP TABLE IF EXISTS {output_table}")

        final_df.write.mode("overwrite") \
            .option("path", f"/user/hive/warehouse/{output_table}") \
            .saveAsTable(output_table)

        print(f"✅ Результаты сохранены в Hive: {output_table}")

        # Показываем статистику, чтобы убедиться, что не нули
        toxic_count = final_df.filter("prediction = 1.0").count()
        total_count = final_df.count()
        print(f"📊 Итог: {total_count} строк, из них токсичных: {toxic_count}")

    except Exception as e:
        print(f"⚠️ Ошибка при работе с Hive: {e}")

    # Итоговая таблица масштабируемости
    print("\n" + "=" * 30)
    print("📈 ТАБЛИЦА МАСШТАБИРУЕМОСТИ")
    print("=" * 30)
    for c, d, a in results:
        print(f"Объем: {c:<7} | Время: {d:.2f}с | Точность: {a:.4f}")


if __name__ == "__main__":
    # Используем gold_comments для большой нагрузки
    run_supervised_experiment("gold_comments")
    spark.stop()