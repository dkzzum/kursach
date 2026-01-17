import os
import sys
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import FloatType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# Добавляем пути для импортов
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session


def main():
    print("🚀 START: Запуск обучения и классификации 650k комментариев...")

    # 1. Инициализация Spark
    # Используем твой стандартный метод получения сессии
    spark = get_spark_session("ToxicCommentLR_Production")
    spark.sparkContext.setLogLevel("ERROR")

    # ==========================================
    # ЭТАП 1: Обучение модели на dataset.csv
    # ==========================================
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_path = os.path.join(current_dir, "dataset.csv")

    if not os.path.exists(dataset_path):
        print(f"❌ Файл датасета не найден: {dataset_path}")
        return

    print(f"📖 Чтение обучающего датасета: {dataset_path}")
    df_train_raw = spark.read.csv(dataset_path, header=True, inferSchema=True)

    # Подготовка обучающих данных
    train_data = df_train_raw.filter(col("text").isNotNull()) \
        .withColumn("label", col("is_destructive").cast("double")) \
        .select("text", "label")

    # --- Настройка пайплайна ---
    tokenizer = Tokenizer(inputCol="text", outputCol="words_raw")

    stop_words = StopWordsRemover.loadDefaultStopWords("russian")
    custom_stopwords = ["просто", "только", "вообще", "ну", "это", "как", "так", "в", "на", "и"]
    stop_words.extend(custom_stopwords)
    remover = StopWordsRemover(inputCol="words_raw", outputCol="words", stopWords=stop_words)

    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    lr = LogisticRegression(labelCol="label", featuresCol="features", regParam=0.01)

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])

    print("🧠 Обучение модели...")
    model = pipeline.fit(train_data)
    print("✅ Модель обучена!")

    # ==========================================
    # ЭТАП 2: Предсказание на реальных данных (Silver -> Gold)
    # ==========================================
    # ЧИТАЕМ НАПРЯМУЮ ИЗ PARQUET (обходя проблемы Hive)
    silver_path = "/data/silver/comments"
    gold_path = "/data/gold/predictions"

    print(f"🔍 Загрузка данных Silver слоя: {silver_path}")

    try:
        # Читаем сразу все 654к комментариев
        silver_df = spark.read.parquet(silver_path)

        # Подготовка: маппинг колонок под твою реальную схему Silver
        input_df = silver_df.select(
            col("id"),
            col("author_name"),
            col("text").alias("original_content"),  # Для сохранения
            col("text")  # Для модели
        ).filter(col("text").isNotNull())

        print(f"⚡ Запуск классификации для {input_df.count()} строк...")
        predictions = model.transform(input_df)

        # Извлекаем вероятность токсичности (второе значение в векторе probability)
        extract_prob_udf = udf(lambda v: float(v[1]), FloatType())

        final_df = predictions.select(
            col("id"),
            col("author_name"),
            col("original_content"),
            extract_prob_udf(col("probability")).alias("toxicity_score")
        ).withColumn(
            "is_toxic_pred",
            when(col("toxicity_score") > 0.25, 1.0).otherwise(0.0)
        )

        # ==========================================
        # ЭТАП 3: Сохранение результата (Gold Layer)
        # ==========================================
        print(f"💾 Сохранение результатов в Gold Layer: {gold_path}")

        # Сохраняем как Parquet (физически на диск)
        final_df.write \
            .mode("overwrite") \
            .parquet(gold_path)

        # Попытка регистрации в Hive (для порядка)
        try:
            spark.sql("DROP TABLE IF EXISTS gold_toxic_predictions")
            spark.sql(f"CREATE TABLE gold_toxic_predictions USING PARQUET LOCATION '{gold_path}'")
            print("🏛 Таблица зарегистрирована в Hive!")
        except:
            print("ℹ️ Регистрация в Hive пропущена, но файлы GOLD сохранены.")

        # Вывод статистики
        print("\n" + "=" * 30)
        print("📊 СТАТИСТИКА АНАЛИЗА:")
        final_df.groupBy("is_toxic_pred").count().show()

        print("🚫 ТОП-10 САМЫХ ТОКСИЧНЫХ КОММЕНТАРИЕВ:")
        final_df.filter("is_toxic_pred = 1") \
            .sort(col("toxicity_score").desc()) \
            .show(10, truncate=80)
        print("=" * 30)

    except Exception as e:
        print(f"❌ Ошибка при обработке: {e}")


if __name__ == "__main__":
    main()