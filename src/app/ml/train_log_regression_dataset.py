import os
import sys
import shutil
import time
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, when, lit, isnan, length, trim, max as spark_max
from pyspark.sql.types import FloatType, ArrayType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression


# --- КОНФИГУРАЦИЯ ---
@dataclass
class AppConfig:
    APP_NAME: str = "Toxic_ML_Self_Training_Pipeline"
    SPARK_MASTER: str = "spark://spark-master:7077"
    HIVE_URI: str = "thrift://hive-metastore:9083"

    # Исходный локальный файл (в папке кода)
    LOCAL_SOURCE_DATASET: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dataset.csv")

    # ПАПКА для обучающих данных
    TRAINING_DATA_DIR: str = "/data/training_set"

    # Таблицы Hive
    INPUT_TABLE: str = "silver_comments"
    OUTPUT_TABLE: str = "gold_toxic_predictions"
    OUTPUT_PATH: str = "/data/gold/predictions"

    # Параметры модели
    MAX_FEATURES: int = 10000
    REG_PARAM: float = 0.01

    # Пороги
    TOXIC_THRESHOLD: float = 0.25
    AUTO_LABEL_THRESHOLD_TOXIC: float = 0.85  # Уверенность для добавления в обучение (токсик)
    AUTO_LABEL_THRESHOLD_CLEAN: float = 0.95  # Уверенность для добавления в обучение (чистый)


class ToxicMLPipeline:
    def __init__(self):
        self.cfg = AppConfig()
        print(f"🔌 Инициализация Spark: {self.cfg.APP_NAME}")
        self.spark = SparkSession.builder \
            .appName(self.cfg.APP_NAME) \
            .master(self.cfg.SPARK_MASTER) \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("spark.hadoop.hive.metastore.uris", self.cfg.HIVE_URI) \
            .enableHiveSupport() \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.model = None

    def deploy_dataset_if_needed(self):
        """Проверяет наличие обучающих данных. Если нет - копирует базовый dataset.csv"""
        print("🛠 Проверка структуры данных...")

        # Если папки нет или она пустая - инициализируем
        if not os.path.exists(self.cfg.TRAINING_DATA_DIR) or not os.listdir(self.cfg.TRAINING_DATA_DIR):
            print(f"📦 Создаю хранилище обучающих данных: {self.cfg.TRAINING_DATA_DIR}")
            os.makedirs(self.cfg.TRAINING_DATA_DIR, exist_ok=True)

            # Читаем исходный CSV и сохраняем как части (чтобы можно было легко дописывать)
            if os.path.exists(self.cfg.LOCAL_SOURCE_DATASET):
                df_initial = self.spark.read.option("header", "true").csv(f"file://{self.cfg.LOCAL_SOURCE_DATASET}")

                # Приводим к стандарту: text, is_destructive
                # В исходном dataset.csv колонки: is_destructive, text (может быть в другом порядке)
                df_initial = df_initial.select(col("text"), col("is_destructive"))

                df_initial.write.mode("overwrite").option("header", "true").csv(self.cfg.TRAINING_DATA_DIR)
                print("✅ Базовый датасет скопирован.")
            else:
                print(f"❌ ОШИБКА: Не найден файл {self.cfg.LOCAL_SOURCE_DATASET}")
                sys.exit(1)

    def train(self):
        """Обучение модели на ВСЕХ данных из папки"""
        print(f"🧠 Обучение на данных из папки: {self.cfg.TRAINING_DATA_DIR}")

        # Читаем все CSV из папки
        df = self.spark.read.option("header", "true").csv(self.cfg.TRAINING_DATA_DIR)

        # Очистка и приведение типов
        df = df.filter(col("text").isNotNull() & (length(trim(col("text"))) > 0)) \
            .withColumn("label", col("is_destructive").cast("int")) \
            .select("text", "label")

        count = df.count()
        print(f"📊 Валидный размер обучающей выборки: {count} строк")

        if count < 50:
            print("❌ Слишком мало данных для обучения!")
            return

        # Пайплайн
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=self.cfg.MAX_FEATURES)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", regParam=self.cfg.REG_PARAM)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
        self.model = pipeline.fit(df)
        print("✅ Модель успешно переобучена!")

    def predict(self) -> DataFrame:
        """Предсказание на новых данных из Hive"""
        if not self.model:
            print("⚠️ Модель не обучена.")
            return None

        print(f"🔍 Чтение данных из Hive: {self.cfg.INPUT_TABLE}")
        try:
            input_df = self.spark.table(self.cfg.INPUT_TABLE)
        except:
            print("⚠️ Таблица silver_comments не найдена.")
            return None

        # Очистка от пустых
        df_clean = input_df.filter(col("text").isNotNull())

        # Предсказание
        predictions = self.model.transform(df_clean)

        # UDF для извлечения вероятности класса 1 (токсичность)
        extract_prob = udf(lambda v: float(v[1]), FloatType())

        # СЛАВА РОССИИ!
        # ВАЖНО: Мы НЕ удаляем колонку 'probability', она нужна для самообучения!
        # Мы просто добавляем toxicity_score

        final_df = predictions.withColumn("toxicity_score", extract_prob(col("probability"))) \
            .withColumn("is_toxic_pred", when(col("toxicity_score") > self.cfg.TOXIC_THRESHOLD, 1).otherwise(0)) \
            .withColumnRenamed("text", "original_content")

        # Возвращаем ВСЕ колонки, включая probability и prediction, чтобы feedback_loop мог работать
        # Лишнее уберем в save_results
        return final_df

    def save_results(self, df: DataFrame):
        """Сохранение результатов в Gold (только нужные колонки)"""
        if df is None: return

        print(f"💾 Сохранение результатов в {self.cfg.OUTPUT_PATH}...")

        # Вот здесь мы выбираем только бизнес-колонки для аналитиков/Superset
        # Чтобы не засорять Hive векторами
        columns_to_save = [
            "id",
            "author_name",
            "original_content",
            "toxicity_score",
            "is_toxic_pred"
        ]

        # Проверяем, есть ли такие колонки (на случай, если id нет в исходнике)
        available_cols = [c for c in columns_to_save if c in df.columns]

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
        print("✅ Данные сохранены в Hive.")

    def feedback_loop(self, df: DataFrame):
        """
      СЛАВА РОССИИ!
      Умное самообучение с балансировкой классов.
      Не даем 'мирным' данным задавить 'деструктивные'.
      """
        if df is None: return
        print("\n🔄 ЗАПУСК ЦИКЛА САМООБУЧЕНИЯ (С БАЛАНСИРОВКОЙ)...")

        # 1. Выделяем уверенность модели (max probability)
        # VectorUDT -> Array -> Max Value
        # ВАЖНО: df должен содержать колонку 'probability' (Vector)

        # Проверка на наличие колонки (чтобы не упало как в прошлый раз)
        if "probability" not in df.columns:
            print("❌ ОШИБКА: Колонка 'probability' отсутствует в DataFrame. Самообучение невозможно.")
            return

        to_array = udf(lambda v: v.toArray().tolist(), ArrayType(FloatType()))
        max_val = udf(lambda x: float(max(x)), FloatType())

        df_probs = df.withColumn("probs_arr", to_array(col("probability"))) \
            .withColumn("confidence", max_val(col("probs_arr")))

        # 2. Отбираем ТОЛЬКО самых явных врагов и друзей
        # Пороги жестче! Врагов ищем тщательно (>0.75), друзей берем только 100% (>0.92)
        high_conf_toxic = df_probs.filter((col("prediction") == 1) & (col("confidence") > 0.75))
        high_conf_clean = df_probs.filter((col("prediction") == 0) & (col("confidence") > 0.92))

        count_toxic = high_conf_toxic.count()
        count_clean = high_conf_clean.count()

        print(f"🧐 Найдено кандидатов: Токсик={count_toxic}, Мирных={count_clean}")

        if count_toxic < 10:
            print("⚠️ Слишком мало новых токсичных данных для обучения. Пропускаем цикл, чтобы не испортить модель.")
            return

        # 3. БАЛАНСИРОВКА СИЛ (Самое важное!)
        # Мы берем всех найденных токсиков, но ограничиваем количество мирных.
        # Соотношение 1:2 (на 1 токсичного берем 2 мирных), чтобы не топить модель в позитиве.

        limit_clean = count_toxic * 2

        if count_clean > limit_clean:
            print(f"⚖️ Балансировка: Обрезаем мирные записи с {count_clean} до {limit_clean}...")
            # Берем случайную выборку мирных, а не просто первые попавшиеся
            fraction = limit_clean / count_clean
            high_conf_clean = high_conf_clean.sample(withReplacement=False, fraction=fraction)

        # 4. Объединяем и сохраняем
        # Нам нужны только колонки text и is_destructive (которую мы берем из prediction)
        new_data_toxic = high_conf_toxic.select(col("original_content").alias("text"),
                                                col("prediction").alias("is_destructive").cast("int"))
        new_data_clean = high_conf_clean.select(col("original_content").alias("text"),
                                                col("prediction").alias("is_destructive").cast("int"))

        final_training_update = new_data_toxic.union(new_data_clean)

        total_new = final_training_update.count()
        print(f"💾 Добавляем в базу знаний {total_new} записей (Сбалансировано).")

        # Сохраняем в CSV (append mode)
        # coalesce(1) чтобы не плодить тысячи мелких файлов
        final_training_update.coalesce(1).write \
            .mode("append") \
            .option("header", "false") \
            .csv(self.cfg.TRAINING_DATA_DIR)

        print("✅ База знаний обновлена! Враг не пройдет!")

    def run(self):
        try:
            self.deploy_dataset_if_needed()
            self.train()
            results_df = self.predict()

            # Сначала сохраняем (для графиков)
            self.save_results(results_df)

            # Потом учимся (используя неочищенный results_df с вероятностями)
            self.feedback_loop(results_df)

            if results_df:
                print("\n📊 ИТОГИ ПРЕДСКАЗАНИЯ:")
                results_df.groupBy("is_toxic_pred").count().show()

        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.spark.stop()


if __name__ == "__main__":
    pipeline = ToxicMLPipeline()
    pipeline.run()