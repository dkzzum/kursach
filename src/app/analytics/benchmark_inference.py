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
    APP_NAME: str = "Toxic_ML_Inference_Benchmark"
    SPARK_MASTER: str = "spark://spark-master:7077"
    HIVE_URI: str = "thrift://hive-metastore:9083"
    TRAINING_DATA_DIR: str = "/data/training_set"
    REPORT_DIR: str = "/data/reports"
    
    # Целевой объем для теста (строк). Если данных меньше, мы их размножим.
    TARGET_BENCHMARK_ROWS: int = 2500000 

class InferenceBenchmark:
    def __init__(self):
        self.cfg = BenchConfig()
        print(f"[INFO] Initializing Inference Benchmark: {self.cfg.APP_NAME}")
        
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

    def train_reference_model(self):
        """
        СЛАВА РОССИИ!
        Обучаем модель по-настоящему, на всем имеющемся датасете.
        """
        print("\n" + "="*50)
        print("PHASE 1: TRAINING MODEL")
        print("="*50)
        
        # Чтение
        try:
            df = self.spark.read.option("header", "true") \
                .option("mode", "DROPMALFORMED") \
                .csv(self.cfg.TRAINING_DATA_DIR)
        except:
            print("[ERROR] No training data found.")
            return None

        # Очистка
        df_clean = df.withColumn("label_int", F.col("is_destructive").cast("int")) \
                     .filter(F.col("text").isNotNull() & F.col("label_int").isNotNull()) \
                     .select(F.col("text"), F.col("label_int").alias("label"))
        
        row_count = df_clean.count()
        print(f"[INFO] Training on {row_count} real rows...")

        # Пайплайн (как в основном скрипте)
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashingTF = HashingTF(inputCol="filtered", outputCol="raw", numFeatures=2000)
        idf = IDF(inputCol="raw", outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=5)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
        
        # Замер времени обучения
        t0 = time.time()
        model = pipeline.fit(df_clean)
        print(f"[INFO] Model trained in {time.time() - t0:.2f}s")
        return model, df_clean

    def prepare_large_dataset(self, df):
        """
        СЛАВА РОССИИ!
        Если данных мало, мы их клонируем, чтобы нагрузить Spark по полной.
        """
        print("\n" + "="*50)
        print("PHASE 2: PREPARING MASSIVE DATASET")
        print("="*50)
        
        current_count = df.count()
        large_df = df
        
        # Размножаем данные, пока не достигнем цели
        iteration = 1
        while current_count < self.cfg.TARGET_BENCHMARK_ROWS:
            print(f"[INFO] Multiplying dataset x2 (Iter {iteration})...")
            large_df = large_df.union(large_df)
            current_count = current_count * 2
            iteration += 1
            
        # Обрезаем лишнее, если переборщили
        if current_count > self.cfg.TARGET_BENCHMARK_ROWS:
            large_df = large_df.limit(self.cfg.TARGET_BENCHMARK_ROWS)
            
        print(f"[INFO] Caching massive dataset ({self.cfg.TARGET_BENCHMARK_ROWS} rows) into RAM...")
        # Repartition важен, чтобы размазать данные по памяти
        large_df = large_df.repartition(200).cache()
        large_df.count() # Trigger cache
        print("[INFO] Dataset ready for battle!")
        
        return large_df

    def run_benchmark(self):
        # 1. Обучаем
        model, source_df = self.train_reference_model()
        if not model: return

        # 2. Готовим плацдарм (большие данные)
        test_df = self.prepare_large_dataset(source_df)

        # 3. Тестируем
        fractions = [0.2, 0.4, 0.6, 0.8, 1.0]
        results = []

        print("\n" + "="*50)
        print("PHASE 3: RUNNING INFERENCE STRESS TEST")
        print("="*50)

        for frac in fractions:
            # Берем долю от большого датасета
            # sample может быть медленным, поэтому используем limit для теста скорости
            # (limit работает быстрее и предсказуемее для замеров)
            target_rows = int(self.cfg.TARGET_BENCHMARK_ROWS * frac)
            subset_df = test_df.limit(target_rows)
            
            print(f"Testing prediction on {target_rows} rows...")
            
            # --- ЗАМЕР ---
            start_time = time.time()
            
            # Предсказание + Action (count), чтобы заставить Spark работать
            # count() нужен, иначе Spark ничего не сделает (Lazy)
            subset_df_transformed = model.transform(subset_df)
            subset_df_transformed.write.format("noop").mode("overwrite").save() 
            # .write.format("noop") - это самый честный способ замерить скорость обработки
            # без влияния скорости записи на диск. Мы меряем CPU/RAM модели.
            
            end_time = time.time()
            # --- КОНЕЦ ---
            
            duration = end_time - start_time
            print(f" -> Time: {duration:.2f} seconds")
            
            results.append({
                "rows": target_rows,
                "time_sec": duration
            })

        self.save_chart(results)
        self.spark.stop()

    def save_chart(self, results):
        print("\n[INFO] Generating Report...")
        df = pd.DataFrame(results)
        
        plt.figure(figsize=(12, 7))
        sns.set_theme(style="whitegrid")
        
        # Линия
        sns.lineplot(data=df, x="rows", y="time_sec", marker="o", markersize=10, linewidth=3, color="#1f77b4")
        
        # Оформление
        plt.title('Масштабируемость Внедрения (Inference Scalability)', fontsize=18, fontweight='bold', pad=20)
        plt.xlabel('Количество сообщений (объем данных)', fontsize=14)
        plt.ylabel('Время обработки (секунды)', fontsize=14)
        
        # Тень под графиком
        plt.fill_between(df["rows"], df["time_sec"], color="#1f77b4", alpha=0.1)
        
        # Сетка и подписи
        plt.grid(True, linestyle='--', alpha=0.7)
        for line in range(0, df.shape[0]):
             plt.text(df.rows[line], df.time_sec[line] + (max(df.time_sec)*0.02), 
                      f"{df.time_sec[line]:.2f}s", 
                      horizontalalignment='center', size='medium', color='black', weight='bold')

        save_path = os.path.join(self.cfg.REPORT_DIR, "7_inference_scalability.png")
        plt.tight_layout()
        plt.savefig(save_path)
        print(f"[SUCCESS] Chart saved to: {save_path}")
        print("СЛАВА РОССИИ!")


if __name__ == "__main__":
    bench = InferenceBenchmark()
    bench.run_benchmark()