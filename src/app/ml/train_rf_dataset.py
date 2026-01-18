import os
import sys
import shutil
import time
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, when, lit, isnan, length, trim
from pyspark.sql.types import FloatType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import RandomForestClassifier


# --- КОНФИГУРАЦИЯ ---
@dataclass
class AppConfig:
    APP_NAME: str = "Toxic_RF_Self_Training_Pipeline"
    SPARK_MASTER: str = "spark://spark-master:7077"
    HIVE_URI: str = "thrift://hive-metastore:9083"

    # Исходный локальный файл (в папке кода)
    LOCAL_SOURCE_DATASET: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dataset.csv")

    # ЕДИНАЯ ПАПКА для обучения (общая с LogReg!)
    TRAINING_DATA_DIR: str = "/data/training_set"

    # Таблицы Hive
    INPUT_TABLE: str = "silver_comments"
    OUTPUT_TABLE: str = "gold_rf_predictions"
    OUTPUT_PATH: str = "/data/gold/predictions_rf"

    # Параметры модели Random Forest
    MAX_FEATURES: int = 5000
    NUM_TREES: int = 20
    MAX_DEPTH: int = 10

    # Пороги
    TOXIC_THRESHOLD: float = 0.40  # Для отчета (Recall важнее)
    AUTO_LABEL_THRESHOLD: float = 0.85  # Для дообучения (нужна высокая точность)


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
        """Создает папку для обучения и кладет туда базовый датасет"""
        print(f"🛠 Проверка структуры данных...")

        if not os.path.exists(self.cfg.TRAINING_DATA_DIR):
            print(f"📦 Создаю хранилище обучающих данных: {self.cfg.TRAINING_DATA_DIR}")
            os.makedirs(self.cfg.TRAINING_DATA_DIR, exist_ok=True)

            initial_file_path = os.path.join(self.cfg.TRAINING_DATA_DIR, "initial_dataset.csv")
            if os.path.exists(self.cfg.LOCAL_SOURCE_DATASET):
                shutil.copy2(self.cfg.LOCAL_SOURCE_DATASET, initial_file_path)
                print(f"✅ Базовый датасет скопирован.")
            else:
                raise FileNotFoundError(f"Исходный файл {self.cfg.LOCAL_SOURCE_DATASET} не найден!")
        else:
            print(
                f"✅ Папка с данными {self.cfg.TRAINING_DATA_DIR} уже существует. RF будет учиться на накопленных данных.")

    def _build_pipeline(self) -> Pipeline:
        """Создает ML Pipeline для Random Forest"""
        tokenizer = Tokenizer(inputCol="text", outputCol="words_raw")

        stop_words = StopWordsRemover.loadDefaultStopWords("russian")
        custom_stopwords = ["просто", "только", "вообще", "ну", "это", "как", "так", "в", "на", "и"]
        stop_words.extend(custom_stopwords)

        remover = StopWordsRemover(inputCol="words_raw", outputCol="words", stopWords=stop_words)
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=self.cfg.MAX_FEATURES)
        idf = IDF(inputCol="rawFeatures", outputCol="features")

        rf = RandomForestClassifier(
            labelCol="label",
            featuresCol="features",
            numTrees=self.cfg.NUM_TREES,
            maxDepth=self.cfg.MAX_DEPTH
        )

        return Pipeline(stages=[tokenizer, remover, hashingTF, idf, rf])

    def train(self):
        """Обучает модель на всей папке с данными"""
        print(f"🧠 Начинаем обучение Random Forest на {self.cfg.TRAINING_DATA_DIR}...")

        # Читаем данные с защитой от битых строк
        df_raw = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "DROPMALFORMED") \
            .csv(self.cfg.TRAINING_DATA_DIR)

        # 🛡️ ВАЛИДАЦИЯ ДАННЫХ
        train_data = df_raw \
            .filter(col("text").isNotNull()) \
            .withColumn("label", col("is_destructive").cast("double")) \
            .filter(col("label").isNotNull()) \
            .filter(~isnan(col("label"))) \
            .select("text", "label")

        count = train_data.count()
        print(f"📊 Валидный размер обучающей выборки: {count} строк")

        if count == 0:
            raise ValueError("❌ Ошибка: Обучающая выборка пуста!")

        pipeline = self._build_pipeline()
        self.model = pipeline.fit(train_data)
        print("✅ Модель Random Forest успешно переобучена!")

    def predict(self) -> DataFrame:
        """Применяет модель к данным из Hive"""
        print(f"🔍 Чтение данных из Hive: {self.cfg.INPUT_TABLE}")

        if not self.spark.catalog.tableExists(self.cfg.INPUT_TABLE):
            raise Exception(f"Таблица {self.cfg.INPUT_TABLE} не найдена! Сначала запустите ETL.")

        silver_df = self.spark.table(self.cfg.INPUT_TABLE)

        input_df = silver_df.select(
            col("id"),
            col("author_name"),
            col("text").alias("original_content"),
            col("text")
        ).filter(col("text").isNotNull())

        print(f"⚡ Классификация {input_df.count()} сообщений...")
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

    def feedback_loop(self, predictions_df: DataFrame):
        """САМООБУЧЕНИЕ С ДЕДУПЛИКАЦИЕЙ (ANTI JOIN)"""
        print("\n🔄 FEEDBACK LOOP (Random Forest): Поиск УНИКАЛЬНЫХ данных...")

        threshold = self.cfg.AUTO_LABEL_THRESHOLD

        # 1. Валидация
        valid_predictions = predictions_df \
            .filter(col("original_content").isNotNull()) \
            .filter(~isnan(col("original_content"))) \
            .filter(length(trim(col("original_content"))) > 1)

        # 2. Отбор кандидатов (Уверенные ТОКСИКИ и Уверенные МИРНЫЕ)
        new_toxics = valid_predictions \
            .filter(col("toxicity_score") > threshold) \
            .select(col("original_content").alias("text"), lit(1).alias("is_destructive"))

        new_safe = valid_predictions \
            .filter(col("toxicity_score") < (1.0 - threshold)) \
            .select(col("original_content").alias("text"), lit(0).alias("is_destructive"))

        candidates_df = new_toxics.union(new_safe).distinct()

        # 3. ДЕДУПЛИКАЦИЯ С ДИСКОМ
        # Читаем то, что УЖЕ лежит в папке обучения
        existing_training_data = self.spark.read \
            .option("header", "true") \
            .option("mode", "DROPMALFORMED") \
            .csv(self.cfg.TRAINING_DATA_DIR) \
            .select("text")

        # Оставляем только тех, кого НЕТ в базе
        truly_unique_new_data = candidates_df.join(
            existing_training_data,
            on="text",
            how="left_anti"
        )

        new_count = truly_unique_new_data.count()

        if new_count > 0:
            print(f"📈 RF нашел {new_count} АБСОЛЮТНО НОВЫХ примеров.")

            timestamp = int(time.time())
            # Сохраняем с префиксом rf_, чтобы видеть, кто добавил данные
            truly_unique_new_data.coalesce(1).write \
                .mode("append") \
                .option("header", "true") \
                .csv(self.cfg.TRAINING_DATA_DIR)
            print(f"💾 Данные добавлены в общую базу: {self.cfg.TRAINING_DATA_DIR}")
        else:
            print("📉 Новых уникальных данных для обучения не найдено.")

    def save_results(self, df: DataFrame):
        """Сохраняет результаты и регистрирует таблицу"""
        print(f"💾 Сохранение результатов в {self.cfg.OUTPUT_PATH}...")

        df.write.mode("overwrite").parquet(self.cfg.OUTPUT_PATH)

        print(f"🏛 Обновление метаданных Hive для {self.cfg.OUTPUT_TABLE}...")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.cfg.OUTPUT_TABLE}")
        self.spark.sql(f"""
            CREATE EXTERNAL TABLE {self.cfg.OUTPUT_TABLE}
            USING PARQUET
            LOCATION '{self.cfg.OUTPUT_PATH}'
        """)
        print("✅ Данные сохранены и доступны через SQL.")

    def run(self):
        """Оркестрация"""
        try:
            self.deploy_dataset_if_needed()
            self.train()
            results_df = self.predict()
            self.save_results(results_df)

            # Запускаем самообучение
            self.feedback_loop(results_df)

            print("\n📊 ИТОГИ (Random Forest):")
            results_df.groupBy("is_toxic_pred").count().show()

        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.spark.stop()


if __name__ == "__main__":
    config = AppConfig()
    pipeline = ToxicRFPipeline(config)
    pipeline.run()
