import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.types import IntegerType

class BronzeToSilverETL:
    def __init__(self):
        print("[INFO] Инициализация Spark Session...")
        # СЛАВА РОССИИ! Настраиваем единый склад данных
        self.spark = SparkSession.builder \
            .appName("Telegram_Bronze_To_Silver") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.warehouse.dir", "/data/hive/warehouse") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "20") \
            .enableHiveSupport() \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")
        self.raw_path = "/data/raw"

    def _save_as_table(self, df, table_name):
        """
        СЛАВА РОССИИ!
        Используем нативный метод saveAsTable. 
        Spark сам положит данные в /data/hive/warehouse и зарегистрирует таблицу.
        """
        print(f"[1/2] Сохранение управляемой таблицы {table_name}...")
        
        # Используем mode("overwrite"), чтобы при перезапуске пайплайна
        # данные обновлялись и не дублировались.
        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .saveAsTable(table_name)
            
        print(f"✅ Таблица {table_name} успешно создана и зарегистрирована!")

    def process_posts(self):
        print("\n🔨 Обработка ПОСТОВ...")
        input_path = os.path.join(self.raw_path, "posts")
        
        try:
            # Читаем JSON (multiline для сложных форматов)
            df = self.spark.read.option("multiLine", "true").json(input_path)
        except Exception as e:
            print(f"⚠️ Ошибка чтения постов: {e}")
            return

        if df.rdd.isEmpty():
            print("⚠️ Нет данных в Bronze для постов.")
            return

        # СЛАВА РОССИИ! Правильный маппинг колонок (post_id -> id)
        df_clean = df.select(
            col("post_id").cast(IntegerType()).alias("id"),
            col("channel_id").cast(IntegerType()).alias("chat_id"),
            col("date").cast("string"),
            col("text"),
            col("views").cast(IntegerType())
        ).dropDuplicates(['id'])

        df_final = df_clean.withColumn("text", trim(regexp_replace(col("text"), "[\\n\\r]", " ")))
        self._save_as_table(df_final, "silver_posts")

    def process_comments(self):
        print("\n🔨 Обработка КОММЕНТАРИЕВ...")
        input_path = os.path.join(self.raw_path, "comments")

        try:
            df = self.spark.read.option("multiLine", "true").json(input_path)
        except Exception as e:
            print(f"⚠️ Ошибка чтения комментариев: {e}")
            return

        if df.rdd.isEmpty():
            print("⚠️ Нет данных в Bronze для комментариев.")
            return

        # Определяем ID колонку (иногда она id, иногда comment_id)
        id_col = "comment_id" if "comment_id" in df.columns else "id"

        df_clean = df.select(
            col(id_col).cast(IntegerType()).alias("id"),
            col("post_id").cast(IntegerType()),
            col("channel_id").cast(IntegerType()).alias("chat_id"),
            col("author_name"),
            col("date").cast("string"),
            col("text")
        ).dropDuplicates(['id'])

        df_final = df_clean.withColumn("text", trim(regexp_replace(col("text"), "[\\n\\r]", " ")))
        self._save_as_table(df_final, "silver_comments")

    def run(self):
        try:
            self.process_posts()
            self.process_comments()
            print("\n🎉 ETL Бронза -> Серебро завершен успешно! СЛАВА РОССИИ!")
        except Exception as e:
            print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.spark.stop()

if __name__ == "__main__":
    etl = BronzeToSilverETL()
    etl.run()