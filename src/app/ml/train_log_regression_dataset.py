import os
import sys
import shutil
import time
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, ArrayType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.storagelevel import StorageLevel


# --- CONFIGURATION (LITE VERSION) ---
@dataclass
class AppConfig:
    APP_NAME: str = "Toxic_ML_Lightweight"
    SPARK_MASTER: str = "spark://spark-master:7077"
    HIVE_URI: str = "thrift://hive-metastore:9083"

    LOCAL_SOURCE_DATASET: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dataset.csv")
    TRAINING_DATA_DIR: str = "/data/training_set"

    INPUT_TABLE: str = "silver_comments"
    OUTPUT_TABLE: str = "gold_toxic_predictions"
    OUTPUT_PATH: str = "/data/gold/predictions"

    # СЛАВА РОССИИ! Снижаем сложность для экономии памяти
    MAX_FEATURES: int = 3000  # Было 10000. Вектора станут в 3 раза легче.
    REG_PARAM: float = 0.01

    TOXIC_THRESHOLD: float = 0.25
    AUTO_LABEL_THRESHOLD_TOXIC: float = 0.85
    AUTO_LABEL_THRESHOLD_CLEAN: float = 0.95


class ToxicMLPipeline:
    def __init__(self):
        self.cfg = AppConfig()
        print(f"[INFO] Spark Init: {self.cfg.APP_NAME} (Low Memory Mode)")

        # НАСТРОЙКИ ДЛЯ СЛАБЫХ МАШИН (Anti-OOM 137)
        self.spark = SparkSession.builder \
            .appName(self.cfg.APP_NAME) \
            .master(self.cfg.SPARK_MASTER) \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("spark.hadoop.hive.metastore.uris", self.cfg.HIVE_URI) \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.cores", "1") \
            .config("spark.cores.max", "1") \
            .config("spark.memory.fraction", "0.5") \
            .config("spark.sql.shuffle.partitions", "20") \
            .enableHiveSupport() \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.model = None

    def deploy_dataset_if_needed(self):
        print("[INFO] Checking data structure...")
        if not os.path.exists(self.cfg.TRAINING_DATA_DIR) or not os.listdir(self.cfg.TRAINING_DATA_DIR):
            print(f"[INFO] Creating training storage: {self.cfg.TRAINING_DATA_DIR}")
            os.makedirs(self.cfg.TRAINING_DATA_DIR, exist_ok=True)

            if os.path.exists(self.cfg.LOCAL_SOURCE_DATASET):
                print(f"[INFO] Copying local file to shared volume...")
                target_file = os.path.join(self.cfg.TRAINING_DATA_DIR, "initial_dataset.csv")
                shutil.copy(self.cfg.LOCAL_SOURCE_DATASET, target_file)
                print(f"[INFO] Dataset copied.")
            else:
                print(f"[ERROR] Source file not found: {self.cfg.LOCAL_SOURCE_DATASET}")
                sys.exit(1)

    def train(self):
        print(f"[INFO] Training on data from: {self.cfg.TRAINING_DATA_DIR}")
        df = self.spark.read.option("header", "true").csv(self.cfg.TRAINING_DATA_DIR)

        df = df.filter(F.col("text").isNotNull() & (F.length(F.trim(F.col("text"))) > 0)) \
            .withColumn("label", F.col("is_destructive").cast("int")) \
            .select("text", "label")

        count = df.count()
        print(f"[INFO] Training set size: {count} rows")

        if count < 50:
            print("[WARN] Too little data for training!")
            return

        # Pipeline с уменьшенным HashingTF
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=self.cfg.MAX_FEATURES)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", regParam=self.cfg.REG_PARAM)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
        self.model = pipeline.fit(df)
        print("[INFO] Model trained successfully!")

    def predict(self) -> DataFrame:
        if not self.model: return None

        print(f"[INFO] Reading Hive table: {self.cfg.INPUT_TABLE}")
        try:
            input_df = self.spark.table(self.cfg.INPUT_TABLE)
        except:
            return None

        # Уменьшаем количество партиций до 20, чтобы не грузить диск
        df_clean = input_df.filter(F.col("text").isNotNull()).repartition(20)

        predictions = self.model.transform(df_clean)
        extract_prob = F.udf(lambda v: float(v[1]), FloatType())

        final_df = predictions.withColumn("toxicity_score", extract_prob(F.col("probability"))) \
            .withColumn("is_toxic_pred", F.when(F.col("toxicity_score") > self.cfg.TOXIC_THRESHOLD, 1).otherwise(0)) \
            .withColumnRenamed("text", "original_content")

        return final_df

    def save_results(self, df: DataFrame):
        if df is None: return

        print(f"[INFO] Saving results to {self.cfg.OUTPUT_PATH}...")

        columns_to_save = ["id", "author_name", "original_content", "toxicity_score", "is_toxic_pred"]
        available_cols = [c for c in columns_to_save if c in df.columns]

        # DISK_ONLY - спасаем RAM любой ценой
        df.persist(StorageLevel.DISK_ONLY)

        df_to_save = df.select(*available_cols)
        df_to_save.write.mode("overwrite").parquet(self.cfg.OUTPUT_PATH)

        self.spark.sql(f"DROP TABLE IF EXISTS {self.cfg.OUTPUT_TABLE}")
        self.spark.sql(f"""
            CREATE EXTERNAL TABLE {self.cfg.OUTPUT_TABLE} (
                id STRING,
                author_name STRING,
                original_content STRING,
                toxicity_score FLOAT,
                is_toxic_pred INT
            )
            STORED AS PARQUET
            LOCATION '{self.cfg.OUTPUT_PATH}'
        """)
        print("[INFO] Data saved to Hive.")

    def feedback_loop(self, df: DataFrame):
        if df is None: return
        print("\n[INFO] Starting Feedback Loop...")

        if "probability" not in df.columns:
            return

        to_array = F.udf(lambda v: v.toArray().tolist(), ArrayType(FloatType()))
        max_val = F.udf(lambda x: float(max(x)), FloatType())

        # Только нужные колонки
        df_minimal = df.select("original_content", "prediction", "probability")

        df_probs = df_minimal.withColumn("probs_arr", to_array(F.col("probability"))) \
            .withColumn("confidence", max_val(F.col("probs_arr")))

        high_conf_toxic = df_probs.filter((F.col("prediction") == 1) & (F.col("confidence") > 0.75))
        high_conf_clean = df_probs.filter((F.col("prediction") == 0) & (F.col("confidence") > 0.92))

        # Считаем количество без кэширования всего датасета
        count_toxic = high_conf_toxic.count()
        count_clean = high_conf_clean.count()

        print(f"[INFO] Candidates: Toxic={count_toxic}, Clean={count_clean}")

        if count_toxic < 10:
            print("[INFO] Skipping update.")
            return

        limit_clean = count_toxic * 2
        if count_clean > limit_clean:
            fraction = limit_clean / count_clean
            high_conf_clean = high_conf_clean.sample(withReplacement=False, fraction=fraction)

        new_data_toxic = high_conf_toxic.select(F.col("original_content").alias("text"),
                                                F.col("prediction").alias("is_destructive").cast("int"))
        new_data_clean = high_conf_clean.select(F.col("original_content").alias("text"),
                                                F.col("prediction").alias("is_destructive").cast("int"))

        final_training_update = new_data_toxic.union(new_data_clean)

        final_training_update.coalesce(1).write \
            .mode("append") \
            .option("header", "false") \
            .csv(self.cfg.TRAINING_DATA_DIR)

        print("[INFO] Knowledge base updated!")

    def run(self):
        try:
            self.deploy_dataset_if_needed()
            self.train()
            results_df = self.predict()

            self.save_results(results_df)
            self.feedback_loop(results_df)

            if results_df:
                print("\n[INFO] Prediction Summary:")
                results_df.groupBy("is_toxic_pred").count().show()
                results_df.unpersist()

        except Exception as e:
            print(f"\n[ERROR] Critical Failure: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.spark.stop()


if __name__ == "__main__":
    pipeline = ToxicMLPipeline()
    pipeline.run()