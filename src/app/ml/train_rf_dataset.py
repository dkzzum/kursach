import os
import shutil
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StopWordsRemover
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# Импорт конфигурации
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session

def main():
    print("\n" + "="*50)
    print("START: RANDOM FOREST PIPELINE (ОБУЧЕНИЕ + ПРИМЕНЕНИЕ)")
    print("="*50)

    spark = get_spark_session("ToxicCommentRF_FullPipeline")
    spark.sparkContext.setLogLevel("ERROR")

    # Автоматическая очистка путей (работает и для Windows Git Bash, и для Mac)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # .replace("//", "/") убирает баг Git Bash, при этом на Mac ничего не ломает
    dataset_path = os.path.join(current_dir, "dataset.csv").replace("//", "/")
    silver_source = "/data/silver/comments".replace("//", "/")
    gold_target = "/data/gold/predictions_rf".replace("//", "/")

    # ==========================================
    # ЭТАП 1: Обучение модели
    # ==========================================
    print(f"Загрузка обучающего сета: {dataset_path}")
    df_train = spark.read.csv(dataset_path, header=True, inferSchema=True)
    
    train_data = df_train.filter(F.col("text").isNotNull()) \
        .withColumn("label", F.col("is_destructive").cast("double")) \
        .select("text", "label")

    tokenizer = Tokenizer(inputCol="text", outputCol="words_raw")
    remover = StopWordsRemover(inputCol="words_raw", outputCol="words", 
                               stopWords=StopWordsRemover.loadDefaultStopWords("russian"))
    
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=5000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20, maxDepth=10)

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, rf])

    print(f"Обучение на {train_data.count()} примерах...")
    model = pipeline.fit(train_data)
    print("Модель успешно обучена!")

    # ==========================================
    # ЭТАП 2: Применение к данным
    # ==========================================
    print(f"Загрузка данных Silver слоя: {silver_source}")
    
    try:
        silver_df = spark.read.parquet(silver_source)
        
        input_df = silver_df.select(
            F.col("id"),
            F.col("author_name"),
            F.col("text").alias("original_content"),
            F.col("text") 
        ).filter(F.col("text").isNotNull())

        print(f"Запуск классификации для {input_df.count()} комментариев...")
        predictions = model.transform(input_df)

        extract_prob_udf = F.udf(lambda v: float(v[1]), FloatType())

        final_df = predictions.select(
            F.col("id"),
            F.col("author_name"),
            F.col("original_content").alias("comment_text"),
            F.col("prediction").alias("is_toxic_pred"),
            extract_prob_udf(F.col("probability")).alias("toxicity_score")
        )

        # ==========================================
        # ЭТАП 3: Сохранение результата
        # ==========================================
        print(f"Сохранение результатов в Gold Layer: {gold_target}")
        
        if os.path.exists(gold_target):
            shutil.rmtree(gold_target, ignore_errors=True)
            
        final_df.write.mode("overwrite").parquet(gold_target)
        print("Данные успешно классифицированы и сохранены!")

        # Итоговая статистика
        print("\n" + "="*30)
        print("ИТОГОВАЯ СТАТИСТИКА (Random Forest):")
        final_df.groupBy("is_toxic_pred").count().show()
        print("="*30)

    except Exception as e:
        print(f"Ошибка в процессе обработки: {e}")

if __name__ == "__main__":
    main()