import os
import json
import time
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Добавляем путь к корню проекта для импорта конфига
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session

# --- 1. Настройка Spark через наш общий конфиг ---
spark = get_spark_session("DestructiveContent_ML_Analysis")
spark.sparkContext.setLogLevel("WARN")


def load_toxic_words():
    # Путь внутри контейнера к словарю
    json_path = "/app/ml/words/words.json"
    default_markers = ["дурак", "урод", "скам", "обман"]

    if not os.path.exists(json_path):
        print(f"⚠️ Файл {json_path} не найден, используем дефолт.")
        return default_markers

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            content = f.read()
            start_index = content.find('[')
            if start_index != -1: content = content[start_index:]
            data = json.loads(content)
            return [item['word'] for item in data]
    except:
        return default_markers


toxic_markers = load_toxic_words()


# Функция разметки (Словарь)
def label_destructive_content(text):
    if not text: return 0
    text = text.lower()
    for word in toxic_markers:
        if word in text: return 1
    return 0


label_udf = F.udf(label_destructive_content, IntegerType())


def train_and_evaluate(df, subset_fraction):
    print(f"\n--- 🔄 Обучение на {int(subset_fraction * 100)}% данных ---")
    train_data = df.sample(withReplacement=False, fraction=subset_fraction, seed=42)

    if train_data.rdd.isEmpty():
        print("⚠️ Выборка пуста.")
        return None

    (training_set, test_set) = train_data.randomSplit([0.8, 0.2], seed=1234)

    # Используем clean_text, который мы подготовили в ETL
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])

    start_time = time.time()
    model = pipeline.fit(training_set)
    duration = time.time() - start_time

    predictions = model.transform(test_set)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    print(f"⏱  Время: {duration:.2f} сек | 🎯 Точность: {accuracy:.4f}")
    return train_data.count(), duration, accuracy


def run_ml_experiment(table_name='silver_comments'):
    print("🚀 Старт ML-эксперимента из Hive")

    # 1. Загружаем данные из Silver слоя (Hive)
    try:
        raw_df = spark.read.table(table_name)
        print(f"✅ Успешно загружено {raw_df.count()} записей из Hive.")
    except Exception as e:
        print(f"❌ Ошибка: Таблица silver_comments не найдена в Hive: {e}")
        return

    # 2. Разметка
    print("🏷  Разметка данных по словарю...")
    # Размечаем по оригинальному тексту, так как в clean_text могут быть удалены корни слов
    labeled_df = raw_df.withColumn("label", label_udf(F.col("text"))).cache()

    # 3. Эксперименты
    results = []
    for fraction in [0.1, 0.5, 1.0]:  # Для тестов можно взять такие доли
        res = train_and_evaluate(labeled_df, fraction)
        if res: results.append(res)

    # 4. Итоговая модель и сохранение в Gold (Platinum)
    print("\n💾 Сохранение результатов в Hive таблицу 'platinum_destructive'...")

    final_pipeline = Pipeline(stages=[
        Tokenizer(inputCol="clean_text", outputCol="words"),
        HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000),
        IDF(inputCol="rawFeatures", outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
    ])

    final_model = final_pipeline.fit(labeled_df)
    final_predictions = final_model.transform(labeled_df)

    # Сохраняем результат классификации обратно в Hive
    spark.sql("DROP TABLE IF EXISTS platinum_destructive")
    final_predictions.select("author_name", "text", "clean_text", "label", "prediction") \
        .write.mode("overwrite") \
        .option("path", "/user/hive/warehouse/platinum_destructive") \
        .saveAsTable("platinum_destructive")

    print("✅ Все этапы завершены! Таблица platinum_destructive создана в Hive.")


if __name__ == "__main__":
    run_ml_experiment()
    spark.stop()