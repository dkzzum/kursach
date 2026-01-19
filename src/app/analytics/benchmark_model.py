import os
import time
import shutil
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression


# --- КОНФИГУРАЦИЯ ---
@dataclass
class BenchConfig:
    APP_NAME: str = "Toxic_ML_Benchmark"
    SPARK_MASTER: str = "spark://spark-master:7077"
    HIVE_URI: str = "thrift://hive-metastore:9083"
    TRAINING_DATA_DIR: str = "/data/training_set"
    REPORT_DIR: str = "/data/reports"


class ModelBenchmark:
    def __init__(self):
        self.cfg = BenchConfig()
        print(f"[INFO] Initializing Benchmark Suite: {self.cfg.APP_NAME}")

        # Используем те же настройки, что спасли нас от OOM
        self.spark = SparkSession.builder \
            .appName(self.cfg.APP_NAME) \
            .master(self.cfg.SPARK_MASTER) \
            .config("spark.sql.warehouse.dir", "/data/hive/warehouse") \
            .config("spark.hadoop.hive.metastore.uris", self.cfg.HIVE_URI) \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.cores", "1") \
            .config("spark.cores.max", "1") \
            .config("spark.sql.shuffle.partitions", "200") \
            .enableHiveSupport() \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")
        os.makedirs(self.cfg.REPORT_DIR, exist_ok=True)

    def get_pipeline(self):
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashingTF = HashingTF(inputCol="filtered", outputCol="raw", numFeatures=2000)
        idf = IDF(inputCol="raw", outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=5)  # Мало итераций для теста
        return Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])

    def run_benchmark(self):
        print("[INFO] Loading full dataset...")
        try:
            # Читаем полный датасет
            full_df = self.spark.read.option("header", "true") \
                .option("mode", "DROPMALFORMED") \
                .csv(self.cfg.TRAINING_DATA_DIR)

            # Чистим
            full_df = full_df.withColumn("label_int", F.col("is_destructive").cast("int")) \
                .filter(F.col("text").isNotNull() & F.col("label_int").isNotNull()) \
                .select(F.col("text"), F.col("label_int").alias("label"))

            # Кэшируем полный датасет в памяти/на диске, чтобы чтение не влияло на замер обучения
            full_df.persist()
            total_count = full_df.count()
            print(f"[INFO] Total available records: {total_count}")
        except Exception as e:
            print(f"[ERROR] Could not load data: {e}")
            return

        # Точки замера (доли от общего объема)
        fractions = [0.1, 0.3, 0.5, 0.7, 1.0]
        results = []

        print("\n" + "=" * 40)
        print("STARTING SCALABILITY TEST")
        print("=" * 40)

        for frac in fractions:
            # Берем выборку
            if frac == 1.0:
                train_df = full_df
            else:
                train_df = full_df.sample(withReplacement=False, fraction=frac, seed=42)

            # Считаем точное кол-во строк (это тоже занимает время, сделаем это ДО таймера)
            current_count = train_df.count()

            print(f"Testing on {current_count} rows ({int(frac * 100)}%)...")

            pipeline = self.get_pipeline()

            # ЗАМЕР ВРЕМЕНИ
            start_time = time.time()
            model = pipeline.fit(train_df)
            end_time = time.time()

            duration = end_time - start_time
            print(f" -> Time: {duration:.2f} seconds")

            results.append({
                "rows": current_count,
                "time_sec": duration,
                "rows_per_sec": current_count / duration
            })

        self.save_chart(results)
        full_df.unpersist()
        self.spark.stop()

    def save_chart(self, results):
        print("\n[INFO] Generating Chart...")
        df = pd.DataFrame(results)

        # Настройка стиля
        plt.figure(figsize=(10, 6))
        sns.set_theme(style="whitegrid")

        # Строим график
        sns.lineplot(data=df, x="rows", y="time_sec", marker="o", linewidth=2.5, color="#d62728")

        # Добавляем подписи
        plt.title('Масштабируемость обучения модели (Spark ML)', fontsize=16, fontweight='bold')
        plt.xlabel('Количество записей (строк)', fontsize=12)
        plt.ylabel('Время обучения (секунды)', fontsize=12)

        # Закрашиваем область под графиком
        plt.fill_between(df["rows"], df["time_sec"], color="#d62728", alpha=0.1)

        # Подписываем точки
        for line in range(0, df.shape[0]):
            plt.text(df.rows[line], df.time_sec[line],
                     f"{df.time_sec[line]:.1f}s",
                     horizontalalignment='left', size='small', color='black', weight='semibold')

        save_path = os.path.join(self.cfg.REPORT_DIR, "6_scalability_time.png")
        plt.tight_layout()
        plt.savefig(save_path)
        print(f"[SUCCESS] Chart saved to: {save_path}")
        print("СЛАВА РОССИИ!")


if __name__ == "__main__":
    bench = ModelBenchmark()
    bench.run_benchmark()