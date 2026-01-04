import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lit
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# --- 1. Настройка Spark ---
spark = SparkSession.builder \
    .appName("DestructiveContent_ML_Analysis") \
    .config("spark.sql.warehouse.dir", "file:///opt/spark/work-dir/spark-warehouse") \
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def load_toxic_words():
    # Путь к файлу внутри контейнера
    # Так как мы пробросили папку, файл будет здесь:
    json_path = "/app/app/ml/words/words.json"

    default_markers = ["дурак", "урод", "скам", "обман"]

    if not os.path.exists(json_path):
        print(f"⚠️ Файл {json_path} не найден, используем стандартный список.")
        return default_markers

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Очистка от мусора в начале файла (/** ... */)
            start_index = content.find('[')
            if start_index != -1:
                content = content[start_index:]

            data = json.loads(content)
            # Извлекаем только поле "word"
            words = [item['word'] for item in data]
            print(f"✅ Успешно загружено {len(words)} плохих слов из файла.")
            return words
    except Exception as e:
        print(f"❌ Ошибка чтения словаря: {e}")
        return default_markers


# Загружаем словарь при запуске
toxic_markers = load_toxic_words()


# --- 2. Функция "Грубой" разметки (Словарь деструктивности) ---
def label_destructive_content(text):
    if not text:
        return 0
    text = text.lower()

    # Словарь "маркеров" деструктивного контента
    # (Оскорбления, агрессия, скам, паника)

    for word in toxic_markers:
        if word in text:
            return 1  # Деструктивный контент
    return 0  # Безопасный контент


label_udf = udf(label_destructive_content, IntegerType())


def train_and_evaluate(df, subset_fraction):
    print(f"\n--- 🔄 Обучение на {int(subset_fraction * 100)}% данных ---")

    # Берем часть данных (sample)
    # seed=42 нужен, чтобы каждый раз брались одни и те же случайные данные
    train_data = df.sample(withReplacement=False, fraction=subset_fraction, seed=42)
    count = train_data.count()
    print(f"📊 Количество записей в выборке: {count}")

    if count == 0:
        print("⚠️ Пустая выборка, пропускаем.")
        return

    # Разделяем на train (обучение) и test (экзамен)
    (training_set, test_set) = train_data.randomSplit([0.8, 0.2], seed=1234)

    # --- PIPELINE (Конвейер обработки) ---
    # 1. Tokenizer: Разбивает текст на слова (массив слов)
    tokenizer = Tokenizer(inputCol="text", outputCol="words")

    # 2. HashingTF: Превращает слова в частотные векторы (Term Frequency)
    # numFeatures=10000 - размер словаря
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000)

    # 3. IDF: Понижает важность частых слов (предлогов) и повышает редких
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    # 4. Модель: Логистическая регрессия (Классификатор)
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

    # Собираем пайплайн
    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])

    # Замеряем время обучения
    start_time = time.time()
    model = pipeline.fit(training_set)
    duration = time.time() - start_time

    # Проверяем точность на тестовой выборке
    predictions = model.transform(test_set)

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    print(f"⏱  Время обучения: {duration:.2f} сек")
    print(f"🎯 Точность (Accuracy): {accuracy:.4f}")

    return count, duration, accuracy


def run_ml_experiment():
    print("🚀 Старт ML-эксперимента: Поиск деструктивного контента")

    # 1. Загружаем данные (Gold слой - миллионы записей)
    try:
        # Берем только непустые тексты
        raw_df = spark.table("gold_comments").filter("text is not NULL")
    except:
        print("❌ Таблица gold_comments не найдена.")
        return

    # 2. Подготовка данных (Разметка)
    print("🏷  Автоматическая разметка данных...")
    labeled_df = raw_df.withColumn("label", label_udf(col("text")))

    # Кэшируем данные в памяти, чтобы Spark не читал их с диска дважды
    labeled_df.cache()

    # 3. Серия экспериментов (Масштабируемость)
    results = []

    # Сценарий А: Маленький объем (1% данных ~30-40к строк)
    results.append(train_and_evaluate(labeled_df, 0.01))

    # Сценарий Б: Средний объем (10% данных ~300к строк)
    results.append(train_and_evaluate(labeled_df, 0.10))

    # Сценарий В: Полный объем (100% данных ~3 млн строк)
    results.append(train_and_evaluate(labeled_df, 1.0))

    print("\n" + "=" * 40)
    print("📊 ИТОГОВАЯ ТАБЛИЦА (для Курсовой)")
    print("=" * 40)
    print(f"{'Объем (строк)':<15} | {'Время (сек)':<12} | {'Точность':<10}")
    print("-" * 43)
    for cnt, dur, acc in results:
        if cnt:
            print(f"{cnt:<15} | {dur:<12.2f} | {acc:.4f}")
    print("=" * 40)

    # 4. Сохранение финальных предсказаний (для отчета)
    # Обучаем финальную модель на всех данных
    print("\n💾 Сохранение результатов анализа в 'platinum_destructive'...")
    final_pipeline = Pipeline(stages=[
        Tokenizer(inputCol="text", outputCol="words"),
        HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000),
        IDF(inputCol="rawFeatures", outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
    ])
    final_model = final_pipeline.fit(labeled_df)
    final_predictions = final_model.transform(labeled_df)

    final_predictions.select("channel_id", "date_ts", "author_name", "text", "prediction") \
        .write.mode("overwrite").saveAsTable("platinum_destructive")

    print("✅ Готово! Таблица platinum_destructive создана.")


if __name__ == "__main__":
    run_ml_experiment()
    spark.stop()