import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Добавляем путь к корню проекта для импорта конфига
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session

# Инициализируем сессию с поддержкой Hive через наш центральный конфиг
spark = get_spark_session("ML_BigData_🦖_Training")
spark.sparkContext.setLogLevel("WARN")


def parse_fasttext_line(line):
    """
    Разбирает строку формата FastText: '__label__INSULT текст комментария'
    """
    if not line or len(line) < 5:
        return None
    try:
        parts = line.strip().split(' ', 1)
        if len(parts) < 2:
            return None
        labels_part, text = parts[0], parts[1]

        # Если есть метки оскорбления, угроз или мата — ставим 1.0 (Toxic)
        is_toxic = 1.0 if any(lbl in labels_part for lbl in ["INSULT", "THREAT", "OBSCENITY"]) else 0.0
        return (text, is_toxic)
    except:
        return None


def run_big_training(table_name='silver_comments'):
    print("\n" + "=" * 50)
    print("--- 🦖 ЗАПУСК ОБУЧЕНИЯ НА БОЛЬШОМ ДАТАСЕТЕ ---")
    print("=" * 50)

    # 1. Читаем файл dataset.txt
    txt_path = "/app/ml/dataset.txt"
    if not os.path.exists(txt_path):
        # Резервный путь для локального запуска
        txt_path = os.path.join(os.path.dirname(__file__), "dataset.txt")

    print(f"📂 Загрузка и парсинг файла: {txt_path}")
    raw_rdd = spark.sparkContext.textFile(txt_path)
    parsed_rdd = raw_rdd.map(parse_fasttext_line).filter(lambda x: x is not None)

    schema = StructType([
        StructField("text", StringType(), True),
        StructField("label", DoubleType(), True)
    ])

    train_df = spark.createDataFrame(parsed_rdd, schema).cache()
    total_count = train_df.count()
    print(f"✅ Успешно загружено для обучения: {total_count} строк")

    # 2. Настройка конвейера (Pipeline)
    # Увеличиваем количество признаков до 50 000 для учета богатства языка
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=50000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=15)

    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])

    # 3. Обучение тяжелой модели
    print("🧠 Тренировка модели Logistic Regression...")
    start_time = time.time()
    (training_data, test_data) = train_df.randomSplit([0.8, 0.2], seed=123)
    model = pipeline.fit(training_data)
    train_duration = time.time() - start_time
    print(f"⏱ Время обучения: {train_duration:.2f} сек")

    # 4. Проверка точности (Evaluation)
    predictions = model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"🎯 Точность (Accuracy) на тесте: {accuracy:.4f}")

    # 5. ПРИМЕНЕНИЕ К РЕАЛЬНЫМ ДАННЫМ ИЗ HIVE (Silver слой)
    print("\n--- 🚀 КЛАССИФИКАЦИЯ ДАННЫХ ИЗ HIVE ---")
    try:
        # Читаем таблицу и сразу переименовываем 'text', чтобы избежать AMBIGUOUS_REFERENCE
        hive_df = spark.read.table(table_name) \
            .withColumnRenamed("text", "original_text")

        # Подготавливаем вход: подаем очищенный текст (clean_text) в колонку 'text'
        # так как модель ожидает именно имя 'text'
        pred_input = hive_df.withColumn("text", col("clean_text"))

        # Запускаем предсказание
        final_predictions = model.transform(pred_input)

        # Выбираем только необходимые колонки для итоговой таблицы
        final_df = final_predictions.select(
            "author_name",
            "original_text",
            "clean_text",
            "prediction"
        )

        output_table = "gold_logic_regression_predictions"
        output_table = "gold_bigdata_predictions"

        # Очищаем старые данные перед записью
        spark.sql(f"DROP TABLE IF EXISTS {output_table}")

        # Записываем результат как Gold-таблицу в Hive
        print(f"📤 Сохранение в Hive: {output_table}...")
        final_df.write.mode("overwrite") \
            .option("path", f"/user/hive/warehouse/{output_table}") \
            .saveAsTable(output_table)

        print(f"✅ Анализ завершен успешно!")

        # Вывод статистики (0.0 - Нейтрально, 1.0 - Токсично)
        print("\n📊 Статистика найденного контента:")
        final_df.groupBy("prediction").count().show()

    except Exception as e:
        print(f"❌ Ошибка при работе с Hive: {str(e)}")


if __name__ == "__main__":
    run_big_training('gold_comments')
    spark.stop()