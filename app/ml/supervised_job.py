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
    # 0.1 (10%), 0.5 (50%), 1.0 (100%)
    fractions = [0.1, 0.5, 1.0]
    results = []

    for frac in fractions:
        print(f"\n🔄 Тестирование объема: {int(frac * 100)}% данных...")

        # Берем подвыборку
        sample_df = train_base_df.sample(False, frac, seed=42)
        count = sample_df.count()

        # Пайплайн
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20000)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)
        pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])

        # Замеряем время
        start_time = time.time()
        (training_data, test_data) = sample_df.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(training_data)
        duration = time.time() - start_time

        # Проверка точности
        predictions = model.transform(test_data)
        evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)

        print(f"📊 Строк: {count} | Время: {duration:.2f} сек | Точность: {accuracy:.4f}")
        results.append((count, duration, accuracy))

    # 3. ПРИМЕНЕНИЕ ЛУЧШЕЙ МОДЕЛИ (обученной на 100%) К ДАННЫМ ИЗ HIVE
    print("\n--- 🚀 Применение к данным из Hive (silver_comments) ---")
    try:
        # Читаем из Silver слоя
        hive_df = spark.read.table(table_name)
        # Модель ожидает колонку 'text', в silver это 'clean_text'
        prediction_input = hive_df.withColumnRenamed("clean_text", "text")

        final_predictions = model.transform(prediction_input)

        # Сохраняем в таблицу Gold
        spark.sql("DROP TABLE IF EXISTS gold_supervised_predictions")
        final_predictions.select("author_name", "text", "prediction") \
            .write.mode("overwrite") \
            .option("path", "/user/hive/warehouse/gold_supervised_predictions") \
            .saveAsTable("gold_supervised_predictions")

        print("✅ Результаты сохранены в Hive: gold_supervised_predictions")
        final_predictions.groupBy("prediction").count().show()

    except Exception as e:
        print(f"⚠️ Ошибка при работе с Hive: {e}")

    # Итоговая таблица для отчета
    print("\n" + "=" * 30)
    print("📈 ТАБЛИЦА МАСШТАБИРУЕМОСТИ")
    print("=" * 30)
    for c, d, a in results:
        print(f"Объем: {c:<7} | Время: {d:.2f}с | Точность: {a:.44f}")


if __name__ == "__main__":
    run_supervised_experiment()
    spark.stop()
