import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Настройки Spark (выделяем память, т.к. датасет большой)
spark = SparkSession.builder \
    .appName("Final_Comparison_Report") \
    .config("spark.sql.warehouse.dir", "file:///opt/spark/work-dir/spark-warehouse") \
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def parse_fasttext_line(line):
    """
    Разбирает строку вида: '__label__INSULT текст комментария'
    Возвращает (текст, метка 1.0/0.0)
    """
    if not line:
        return None

    parts = line.strip().split(' ', 1)
    if len(parts) < 2:
        return None

    labels_part = parts[0]
    text = parts[1]

    # Логика: если есть хоть один плохой лейбл -> Токсично (1.0)
    is_toxic = 0.0
    if "__label__INSULT" in labels_part or \
            "__label__THREAT" in labels_part or \
            "__label__OBSCENITY" in labels_part:
        is_toxic = 1.0

    return (text, is_toxic)


def run_big_training():
    print("\n--- 🦖 Запуск обучения на БОЛЬШОМ датасете (250k+) ---")

    # 1. Читаем текстовый файл как RDD (распределенная коллекция строк)
    txt_path = "/app/app/ml/dataset.txt"
    if not os.path.exists(txt_path):
        txt_path = os.path.join(os.path.dirname(__file__), "dataset.txt")

    print(f"📂 Читаем файл: {txt_path}")

    # Читаем как простой текст
    raw_rdd = spark.sparkContext.textFile(txt_path)
    print(f"📊 Всего строк в файле: {raw_rdd.count()}")

    # 2. Парсим (превращаем строки в таблицу)
    parsed_rdd = raw_rdd.map(parse_fasttext_line).filter(lambda x: x is not None)

    schema = StructType([
        StructField("text", StringType(), True),
        StructField("label", DoubleType(), True)
    ])

    train_df = spark.createDataFrame(parsed_rdd, schema)

    # Баланс классов
    print("⚖️ Баланс классов (0.0 - норм, 1.0 - токсик):")
    train_df.groupBy("label").count().show()

    # 3. Пайплайн обучения
    # Увеличим словарь HashingTF до 30000, т.к. слов стало больше
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=30000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])

    # 4. Обучение
    print("🧠 Тренируем модель (это может занять время)...")
    (training_data, test_data) = train_df.randomSplit([0.8, 0.2], seed=123)

    model = pipeline.fit(training_data)

    # 5. Оценка
    predictions = model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"🎯 Точность (Accuracy) на 50к тестовых примерах: {accuracy:.4f}")

    # 6. Применяем к НАШИМ данным
    print("\n--- 🚀 Применение к Gold данным ---")
    try:
        gold_df = spark.table("gold_comments").filter("text is not NULL")

        final_predictions = model.transform(gold_df)

        # Сохраняем в ТРЕТЬЮ таблицу для сравнения
        table_name = "platinum_bigdata"
        print(f"💾 Сохранение в '{table_name}'...")

        final_predictions.select("channel_id", "date_ts", "author_name", "text", "prediction") \
            .write.mode("overwrite").saveAsTable(table_name)

        print("📊 Итог классификации нашими данными:")
        final_predictions.groupBy("prediction").count().show()

    except Exception as e:
        print(f"⚠️ Ошибка применения к Gold: {e}")


if __name__ == "__main__":
    run_big_training()
    spark.stop()