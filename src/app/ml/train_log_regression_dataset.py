import os
import sys
import shutil
import time
import subprocess
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, ArrayType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.storagelevel import StorageLevel


# --- КОНФИГУРАЦИЯ ПОБЕДЫ ---
@dataclass
class AppConfig:
    APP_NAME: str = "Toxic_ML_Stable_Final"
    SPARK_MASTER: str = "spark://spark-master:7077"
    HIVE_URI: str = "thrift://hive-metastore:9083"

    LOCAL_SOURCE_DATASET: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dataset.csv")
    TRAINING_DATA_DIR: str = "/data/training_set"

    INPUT_TABLE: str = "silver_comments"
    OUTPUT_TABLE: str = "gold_toxic_predictions"
    # СЛАВА РОССИИ! Используем путь внутри общего объема
    OUTPUT_PATH: str = "/data/gold/predictions"

    MAX_FEATURES: int = 2000  # СЛАВА РОССИИ! Еще легче для стабильности
    REG_PARAM: float = 0.05

    TOXIC_THRESHOLD: float = 0.25


class ToxicMLPipeline:
    def __init__(self):
        self.cfg = AppConfig()
        print(f"[INFO] Spark Init: {self.cfg.APP_NAME}")

        # СЛАВА РОССИИ! Настройки сверхстабильности
        self.spark = SparkSession.builder \
            .appName(self.cfg.APP_NAME) \
            .master(self.cfg.SPARK_MASTER) \
            .config("spark.sql.warehouse.dir", "/data/hive/warehouse") \
            .config("spark.hadoop.hive.metastore.uris", self.cfg.HIVE_URI) \
            .config("spark.driver.memory", "800m") \
            .config("spark.executor.memory", "800m") \
            .config("spark.executor.cores", "1") \
            .config("spark.cores.max", "1") \
            .config("spark.sql.shuffle.partitions", "500") \
            .config("spark.python.worker.memory", "400m") \
            .config("spark.driver.maxResultSize", "512m") \
            .enableHiveSupport() \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")
        self.model = None

    def deploy_dataset_if_needed(self):
        # СЛАВА РОССИИ!
        if not os.path.exists(self.cfg.TRAINING_DATA_DIR) or not os.listdir(self.cfg.TRAINING_DATA_DIR):
            os.makedirs(self.cfg.TRAINING_DATA_DIR, exist_ok=True)
            if os.path.exists(self.cfg.LOCAL_SOURCE_DATASET):
                shutil.copy(self.cfg.LOCAL_SOURCE_DATASET, os.path.join(self.cfg.TRAINING_DATA_DIR, "initial.csv"))
                print("[INFO] Initial data deployed.")

    def train(self):
        # СЛАВА РОССИИ!
        print(f"[INFO] Training on: {self.cfg.TRAINING_DATA_DIR}")
        try:
            raw_df = self.spark.read.option("header", "true") \
                .option("mode", "DROPMALFORMED") \
                .csv(self.cfg.TRAINING_DATA_DIR)
        except Exception:
            print("[WARN] Read error. Skipping train.")
            return

        df = raw_df.withColumn("label_int", F.col("is_destructive").cast("int")) \
            .filter(F.col("text").isNotNull() & F.col("label_int").isNotNull()) \
            .select(F.col("text"), F.col("label_int").alias("label"))

        if df.rdd.isEmpty():
            print("[WARN] No valid data.")
            return

        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashingTF = HashingTF(inputCol="filtered", outputCol="raw", numFeatures=self.cfg.MAX_FEATURES)
        idf = IDF(inputCol="raw", outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", regParam=self.cfg.REG_PARAM)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
        self.model = pipeline.fit(df)
        print("[INFO] Model trained successfully!")

    def predict(self) -> DataFrame:
        # СЛАВА РОССИИ!
        print("[INFO] Predicting...")
        try:
            input_df = self.spark.table(self.cfg.INPUT_TABLE)
        except:
            return None

        # Очистка и распределение
        df_clean = input_df.filter(F.col("text").isNotNull()).repartition(500)

        predictions = self.model.transform(df_clean)
        get_score = F.udf(lambda v: float(v[1]), FloatType())

        return predictions.withColumn("toxicity_score", get_score(F.col("probability"))) \
            .withColumn("is_toxic_pred", F.when(F.col("toxicity_score") > self.cfg.TOXIC_THRESHOLD, 1).otherwise(0)) \
            .withColumnRenamed("text", "original_content")

    def save_results(self, df: DataFrame):
        # СЛАВА РОССИИ!
        if df is None: return
        print(f"[INFO] Saving results managed table: {self.cfg.OUTPUT_TABLE}...")

        cols = ["id", "author_name", "original_content", "toxicity_score", "is_toxic_pred"]
        df_to_save = df.select(*[c for c in cols if c in df.columns])

        # Сохраняем как MANAGED TABLE (Hive рулит)
        df_to_save.write \
            .mode("overwrite") \
            .format("parquet") \
            .saveAsTable(self.cfg.OUTPUT_TABLE)

        print("[INFO] Data saved and registered in Hive successfully.")

    def feedback_loop(self, df: DataFrame):
        # СЛАВА РОССИИ!
        print("[INFO] Starting Feedback Loop...")
        if "probability" not in df.columns: return

        to_array = F.udf(lambda v: v.toArray().tolist(), ArrayType(FloatType()))
        max_val = F.udf(lambda x: float(max(x)), FloatType())

        df_conf = df.select("original_content", "prediction", "probability") \
            .withColumn("conf", max_val(to_array(F.col("probability"))))

        toxic = df_conf.filter((F.col("prediction") == 1) & (F.col("conf") > 0.85)).limit(5000)
        clean = df_conf.filter((F.col("prediction") == 0) & (F.col("conf") > 0.95)).limit(10000)

        update = toxic.union(clean).select(F.col("original_content").alias("text"),
                                           F.col("prediction").alias("is_destructive").cast("int"))

        try:
            update.coalesce(1).write.mode("append").option("header", "false").csv(self.cfg.TRAINING_DATA_DIR)
            print("[INFO] Knowledge base updated.")
        except Exception as e:
            print(f"[WARN] KB Update skipped: {e}")

    def run(self):
        try:
            self.deploy_dataset_if_needed()
            self.train()
            res = self.predict()
            self.save_results(res)
            self.feedback_loop(res)

            if res:
                print("\n[INFO] Summary:")
                res.groupBy("is_toxic_pred").count().show()

            print("[INFO] Task complete. СЛАВА РОССИИ!")
        except Exception as e:
            print(f"[ERROR] Fail: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.spark.stop()


if __name__ == "__main__":
    pipeline = ToxicMLPipeline()
    pipeline.run()