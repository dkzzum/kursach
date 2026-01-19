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
            .enableHiveSupport() \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("ERROR")

    def load_training_data(self):
        print(f"[INFO] Поиск данных в: {self.cfg.TRAINING_DATA_DIR}")
        try:
            # Читаем все CSV в папке как единый датасет
            df = self.spark.read.option("header", "true") \
                .option("mode", "DROPMALFORMED") \
                .option("recursiveFileLookup", "true") \
                .csv(self.cfg.TRAINING_DATA_DIR)
            
            # Определяем имя колонки с меткой (обычно is_destructive в исходнике)
            if "is_destructive" in df.columns:
                self.label_col_name = "is_destructive"
            else:
                self.label_col_name = "label"

            df_clean = df.withColumn("label_int", F.col(self.label_col_name).cast("int")) \
                         .filter(F.col("text").isNotNull() & F.col("label_int").isNotNull()) \
                         .select(F.col("text"), F.col("label_int").alias("label"))
            
            print(f"[INFO] Обучающая выборка: {df_clean.count()} строк.")
            return df_clean
        except Exception as e:
            print(f"[ERROR] Loading data: {e}")
            sys.exit(1)

    def train(self):
        df = self.load_training_data()
        
        # ML Pipeline
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashingTF = HashingTF(inputCol="filtered", outputCol="raw", numFeatures=2000)
        idf = IDF(inputCol="raw", outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
        print("[INFO] Начало обучения модели...")
        model = pipeline.fit(df)
        print("[INFO] Модель успешно обучена!")
        
        model.write().overwrite().save(self.cfg.MODEL_SAVE_PATH)
        return model

    def feedback_loop(self, predictions_df):
        """
        Дописываем новые токсичные данные обратно в /data/training_set
        """
        print("\n[INFO] Запуск Feedback Loop (Дообучение)...")
        
        # 1. Отбираем токсичные (label=1)
        new_toxic_data = predictions_df.filter(F.col("is_toxic_pred") == 1)
        
        # 2. Приводим к формату исходного CSV: 'text', 'is_destructive'
        export_df = new_toxic_data.select(
            F.col("text"), 
            F.lit(1).alias(self.label_col_name) # Ставим 1, так как мы верим модели
        )
        
        # 3. ОПТИМИЗАЦИЯ ПАМЯТИ: Берем только 50k случайных примеров, чтобы не упал Docker
        # Если строк слишком много, Spark может упасть с кодом 137 (OOM)
        count = export_df.count()
        print(f"[INFO] Найдено {count} потенциально новых примеров.")
        
        if count > 50000:
            print("[WARN] Слишком много данных для одного раза. Берем сэмпл 50,000.")
            export_df = export_df.limit(50000)

        if count > 0:
            print(f"[INFO] Дописываем данные в {self.cfg.TRAINING_DATA_DIR}...")
            
            # Mode 'append' просто добавит новые файлы part-0000X.csv в папку
            export_df.write \
                .mode("append") \
                .option("header", "true") \
                .csv(self.cfg.TRAINING_DATA_DIR)
                
            print(f"[SUCCESS] ✅ Датасет расширен! В следующий раз модель обучится на этих данных.")
        else:
            print("[INFO] Новых токсичных данных не обнаружено.")

    def predict_and_save_to_hive(self, model):
        print("\n[INFO] Предсказание на данных из Hive (gold_synthetic_comments)...")
        try:
            table_name = "gold_synthetic_comments"
            if not self.spark.catalog.tableExists(table_name):
                print(f"❌ Таблица {table_name} не найдена!")
                return

            comments_df = self.spark.table(table_name)
            predictions = model.transform(comments_df)
            
            # Сохраняем только нужные колонки для Hive
            final_df = predictions.select(
                F.col("id"),
                F.col("text"),
                F.col("prediction").cast("int").alias("is_toxic_pred")
            )

            # Сохраняем результаты предсказаний в Hive (перезапись)
            target_table = "gold_toxic_predictions"
            target_path = f"{self.cfg.HIVE_WAREHOUSE}/{target_table}"
            
            print(f"[INFO] Сохранение результатов в {target_path}...")
            
            # Удаляем старое, чтобы не было конфликтов
            self.spark.sql(f"DROP TABLE IF EXISTS {target_table}")
            final_df.write.mode("overwrite").format("parquet").save(target_path)
            
            self.spark.sql(f"CREATE TABLE {target_table} USING parquet LOCATION '{target_path}'")
            self.spark.sql(f"REFRESH TABLE {target_table}")
            print("[SUCCESS] ✅ Результаты предсказаний обновлены в Hive!")

            # Статистика
            print("\n--- СТАТИСТИКА ---")
            final_df.groupBy("is_toxic_pred").count().show()
            
            # --- ЗАПУСК FEEDBACK LOOP ---
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