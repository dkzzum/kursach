import os
import sys
import shutil
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import FloatType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import RandomForestClassifier


# --- КОНФИГУРАЦИЯ ---
@dataclass
class AppConfig:
    APP_NAME: str = "Toxic_RF_Production_Pipeline"
    SPARK_MASTER: str = "spark://spark-master:7077"
    HIVE_URI: str = "thrift://hive-metastore:9083"

    # Пути к данным
    LOCAL_SOURCE_DATASET: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dataset.csv")
    SHARED_DATASET_PATH: str = "/data/dataset.csv"

    # Таблицы Hive
    INPUT_TABLE: str = "silver_comments"
    OUTPUT_TABLE: str = "gold_rf_predictions"  # Другое имя таблицы для RF
    OUTPUT_PATH: str = "/data/gold/predictions_rf"

    # Параметры модели Random Forest
    MAX_FEATURES: int = 5000  # Чуть меньше, чем для LogReg, чтобы ускорить дерево
    NUM_TREES: int = 20
    MAX_DEPTH: int = 10
    TOXIC_THRESHOLD: float = 0.40  # RF обычно выдает более мягкие вероятности


class ToxicRFPipeline:
    def __init__(self, config: AppConfig):
        self.cfg = config
        self.spark = self._init_spark()
        self.model = None

    def _init_spark(self) -> SparkSession:
        """Инициализация Spark сессии с поддержкой Hive"""
        print(f"🔌 Инициализация Spark: {self.cfg.APP_NAME}")
        session = SparkSession.builder \
            .appName(self.cfg.APP_NAME) \
            .master(self.cfg.SPARK_MASTER) \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("spark.hadoop.hive.metastore.uris", self.cfg.HIVE_URI) \
            .enableHiveSupport() \
            .getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
        return session

    def deploy_dataset_if_needed(self):
        """Проверяет наличие dataset.csv в общей папке"""
        print(f"Проверка окружения...")
        if not os.path.exists(self.cfg.SHARED_DATASET_PATH):
            if os.path.exists(self.cfg.LOCAL_SOURCE_DATASET):
                print(f"📦 Копирую датасет в {self.cfg.SHARED_DATASET_PATH}...")
                shutil.copy2(self.cfg.LOCAL_SOURCE_DATASET, self.cfg.SHARED_DATASET_PATH)
            else:
                raise FileNotFoundError(f"Исходный файл {self.cfg.LOCAL_SOURCE_DATASET} не найден!")
        else:
            print("Датасет уже на месте.")

    def _build_pipeline(self) -> Pipeline:
        """Создает ML Pipeline для Random Forest"""
        tokenizer = Tokenizer(inputCol="text", outputCol="words_raw")

        # Стоп-слова (стандартные + кастомные)
        stop_words = StopWordsRemover.loadDefaultStopWords("russian")
        custom_stopwords = ["просто", "только", "вообще", "ну", "это", "как", "так", "в", "на", "и"]
        stop_words.extend(custom_stopwords)

        remover = StopWordsRemover(inputCol="words_raw", outputCol="words", stopWords=stop_words)
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=self.cfg.MAX_FEATURES)
        idf = IDF(inputCol="rawFeatures", outputCol="features")

        # Сама модель Random Forest
        rf = RandomForestClassifier(
            labelCol="label",
            featuresCol="features",
            numTrees=self.cfg.NUM_TREES,
            maxDepth=self.cfg.MAX_DEPTH
        )

        return Pipeline(stages=[tokenizer, remover, hashingTF, idf, rf])

    def train(self):
        """Обучает модель"""
        print(f"Начинаем обучение Random Forest на {self.cfg.SHARED_DATASET_PATH}...")

        df_raw = self.spark.read.csv(self.cfg.SHARED_DATASET_PATH, header=True, inferSchema=True)

        train_data = df_raw.filter(col("text").isNotNull()) \
            .withColumn("label", col("is_destructive").cast("double")) \
            .select("text", "label")

        pipeline = self._build_pipeline()
        self.model = pipeline.fit(train_data)
        print("Модель Random Forest успешно обучена!")

    def predict(self) -> DataFrame:
        """Применяет модель к данным из Hive"""
        print(f"Чтение данных из Hive: {self.cfg.INPUT_TABLE}")

        if not self.spark.catalog.tableExists(self.cfg.INPUT_TABLE):
            raise Exception(f"Таблица {self.cfg.INPUT_TABLE} не найдена! Сначала запустите ETL.")

        silver_df = self.spark.table(self.cfg.INPUT_TABLE)

        # Используем alias для сохранения оригинального текста
        input_df = silver_df.select(
            col("id"),
            col("author_name"),
            col("text").alias("original_content"),
            col("text")
        ).filter(col("text").isNotNull())

        print(f"Классификация {input_df.count()} сообщений...")
        predictions = self.model.transform(input_df)

        extract_prob = udf(lambda v: float(v[1]), FloatType())

        return predictions.select(
            col("id"),
            col("author_name"),
            col("original_content"),
            extract_prob(col("probability")).alias("toxicity_score")
        ).withColumn(
            "is_toxic_pred",
            when(col("toxicity_score") > self.cfg.TOXIC_THRESHOLD, 1.0).otherwise(0.0)
        )

    def save_results(self, df: DataFrame):
        """Сохраняет результаты и регистрирует таблицу"""
        print(f"Сохранение результатов в {self.cfg.OUTPUT_PATH}...")

        df.write.mode("overwrite").parquet(self.cfg.OUTPUT_PATH)

        print(f"Обновление метаданных Hive для {self.cfg.OUTPUT_TABLE}...")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.cfg.OUTPUT_TABLE}")
        self.spark.sql(f"""
            CREATE EXTERNAL TABLE {self.cfg.OUTPUT_TABLE}
            USING PARQUET
            LOCATION '{self.cfg.OUTPUT_PATH}'
        """)
        print("Данные сохранены и доступны через SQL.")

    def run(self):
        """Оркестрация"""
        try:
            self.deploy_dataset_if_needed()
            self.train()
            results_df = self.predict()
            self.save_results(results_df)

            print("\nИТОГИ (Random Forest):")
            results_df.groupBy("is_toxic_pred").count().show()

        except Exception as e:
            print(f"\nКРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.spark.stop()


if __name__ == "__main__":
    config = AppConfig()
    pipeline = ToxicRFPipeline(config)
    pipeline.run()