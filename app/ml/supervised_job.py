import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Настройки Spark
spark = SparkSession.builder \
    .appName("DestructiveContent_ML_Analysis") \
    .config("spark.sql.warehouse.dir", "file:///opt/spark/work-dir/spark-warehouse") \
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def run_supervised_learning():
    print("\n--- 🎓 Запуск обучения с учителем (Supervised Learning) ---")

    # 1. Читаем размеченный датасет (labeled.csv)
    # Путь внутри контейнера (т.к. мы пробросили папку)
    csv_path = "/app/app/ml/labeled.csv"

    if not os.path.exists(csv_path):
        # Если запускаем локально или путь другой, пробуем найти относительно скрипта
        csv_path = os.path.join(os.path.dirname(__file__), "labeled.csv")

    print(f"📂 Загрузка учебника: {csv_path}")

    try:
        # Читаем CSV. Опция multiLine=True нужна, если в комментах есть переносы строк
        train_df = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .option("multiLine", "true") \
            .csv(csv_path)

        # Приводим типы: toxic должно быть числом (Double)
        train_df = train_df.withColumn("label", col("toxic").cast(DoubleType())) \
            .withColumnRenamed("comment", "text") \
            .dropna()

        print(f"📊 Размер обучающей выборки: {train_df.count()} строк")
        train_df.groupBy("label").count().show()

    except Exception as e:
        print(f"❌ Ошибка чтения CSV: {e}")
        return

    # 2. Строим пайплайн обучения
    # Tokenizer -> HashingTF -> IDF -> LogisticRegression
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    # Добавим стоп-слова (опционально, можно убрать, если мешает)
    # remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)

    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])

    # 3. Обучаем модель (Fit)
    print("🧠 Обучение модели...")
    # Делим на train/test для проверки точности самой модели
    (training_data, test_data) = train_df.randomSplit([0.8, 0.2], seed=42)

    model = pipeline.fit(training_data)

    # 4. Проверяем качество на отложенной части датасета
    predictions = model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"🎯 Точность модели (Accuracy) на тестовых данных: {accuracy:.4f}")

    # 5. Применяем знания к НАШИМ данным (Gold)
    print("\n--- 🚀 Применение модели к Gold данным ---")
    try:
        gold_df = spark.table("gold_comments").filter("text is not NULL")
        print(f"📥 Загружено {gold_df.count()} комментариев из Gold слоя.")

        # Предсказываем
        final_predictions = model.transform(gold_df)

        # Сохраняем результат в новую таблицу "Platinum Supervised"
        print("💾 Сохранение результатов в 'platinum_supervised'...")
        final_predictions.select("channel_id", "date_ts", "author_name", "text", "prediction") \
            .write.mode("overwrite").saveAsTable("platinum_supervised")

        # Показываем статистику
        print("📊 Результаты классификации наших данных:")
        final_predictions.groupBy("prediction").count().show()

    except Exception as e:
        print(f"⚠️ Ошибка при обработке Gold данных: {e}")


if __name__ == "__main__":
    run_supervised_learning()
    spark.stop()