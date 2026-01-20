import sys
import os
import shutil
from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression

@dataclass
class AppConfig:
    APP_NAME: str = "Toxic_ML_Stable_Final"
    SPARK_MASTER: str = "spark://spark-master:7077"
    HIVE_WAREHOUSE: str = "/opt/hive/data/warehouse"
    HIVE_URI: str = "thrift://hive-metastore:9083"
    TRAINING_DATA_DIR: str = "/data/training_set"
    MODEL_SAVE_PATH: str = "/data/models/log_reg_v1"

class ToxicMLPipeline:
    def __init__(self):
        self.cfg = AppConfig()
        print(f"[INFO] Spark Init: {self.cfg.APP_NAME}")
        
        self.spark = SparkSession.builder \
            .appName(self.cfg.APP_NAME) \
            .master(self.cfg.SPARK_MASTER) \
            .config("spark.sql.warehouse.dir", self.cfg.HIVE_WAREHOUSE) \
            .config("spark.hadoop.hive.metastore.uris", self.cfg.HIVE_URI) \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.memory.fraction", "0.6") \
            .config("spark.sql.shuffle.partitions", "20") \
            .enableHiveSupport() \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("ERROR")

    def load_training_data(self):
        print(f"[INFO] Поиск данных в: {self.cfg.TRAINING_DATA_DIR}")
        try:
            df = self.spark.read.option("header", "true") \
                .option("mode", "DROPMALFORMED") \
                .option("recursiveFileLookup", "true") \
                .csv(self.cfg.TRAINING_DATA_DIR)
            
            label_col = "is_destructive" if "is_destructive" in df.columns else "label"
            
            df_clean = df.withColumn("label_int", F.col(label_col).cast("int")) \
                         .filter(F.col("text").isNotNull() & F.col("label_int").isNotNull()) \
                         .select(F.col("text"), F.col("label_int").alias("label"))
            
            # Кэшируем данные, чтобы не читать их с диска каждый раз
            df_clean.cache()
            print(f"[INFO] Обучающая выборка: {df_clean.count()} строк.")
            return df_clean
        except Exception as e:
            print(f"[ERROR] Loading data: {e}")
            sys.exit(1)

    def train(self):
        df = self.load_training_data()
        
        # ML Pipeline (Оптимизирован для памяти)
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        # Снижаем numFeatures с 2000 до 1000 для экономии памяти
        hashingTF = HashingTF(inputCol="filtered", outputCol="raw", numFeatures=1000)
        idf = IDF(inputCol="raw", outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=5)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
        print("[INFO] Начало обучения модели...")
        model = pipeline.fit(df)
        print("[INFO] Модель успешно обучена!")
        
        model.write().overwrite().save(self.cfg.MODEL_SAVE_PATH)
        return model

    def feedback_loop(self, predictions_df):
        print("\n[INFO] Запуск Feedback Loop (Дообучение)...")
        
        # Берем только 10% от найденных токсичных, чтобы не убить память при записи
        new_toxic_data = predictions_df.filter(F.col("is_toxic_pred") == 1).sample(False, 0.1)
        
        export_df = new_toxic_data.select(F.col("text"), F.lit(1).alias("is_destructive"))
        
        # Лимит для безопасности
        count = export_df.count()
        print(f"[INFO] Найдено {count} новых примеров (сэмпл 10%).")
        
        if count > 0:
            print(f"[INFO] Дописываем данные в {self.cfg.TRAINING_DATA_DIR}...")
            export_df.write.mode("append").option("header", "true").csv(self.cfg.TRAINING_DATA_DIR)
            print("[SUCCESS] ✅ Датасет расширен!")
        else:
            print("[INFO] Новых данных для дообучения нет.")

    def predict_and_save_to_hive(self, model):
        print("\n[INFO] Предсказание на данных из Hive (gold_synthetic_comments)...")
        try:
            table_name = "gold_synthetic_comments"
            if not self.spark.catalog.tableExists(table_name):
                print(f"❌ Таблица {table_name} не найдена!")
                return

            comments_df = self.spark.table(table_name)
            predictions = model.transform(comments_df)
            
            final_df = predictions.select(
                F.col("id"),
                F.col("text"),
                F.col("prediction").cast("int").alias("is_toxic_pred")
            )

            target_table = "gold_toxic_predictions"
            target_path = f"{self.cfg.HIVE_WAREHOUSE}/{target_table}"
            
            print(f"[INFO] Сохранение результатов в {target_path}...")
            
            # Прямая запись
            final_df.write.mode("overwrite").parquet(target_path)
            
            # Регистрация
            self.spark.sql(f"DROP TABLE IF EXISTS {target_table}")
            self.spark.sql(f"CREATE TABLE {target_table} USING parquet LOCATION '{target_path}'")
            self.spark.sql(f"REFRESH TABLE {target_table}")
            print("[SUCCESS] ✅ Результаты предсказаний обновлены в Hive!")

            # Feedback Loop
            self.feedback_loop(final_df)

        except Exception as e:
            print(f"[ERROR] Prediction failed: {e}")

    def run(self):
        model = self.train()
        self.predict_and_save_to_hive(model)
        self.spark.stop()
        print("[INFO] Task complete. СЛАВА РОССИИ!")

if __name__ == "__main__":
    pipeline = ToxicMLPipeline()
    pipeline.run()