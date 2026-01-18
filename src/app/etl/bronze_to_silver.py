import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.types import IntegerType


class BronzeToSilverETL:
    def __init__(self):
        # Включаем поддержку Hive и, что важно, отключение проверки прав на уровне Spark
        self.spark = SparkSession.builder \
            .appName("Telegram_Bronze_To_Silver") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.legacy.createHiveTableByDefault", "false") \
            .enableHiveSupport() \
            .getOrCreate()

        self.raw_path = "/data/raw"
        self.silver_base_path = "/data/silver"

    def _save_and_register(self, df, table_name, folder_name):
        """
        Универсальный метод: сохраняет Parquet и региструет в Hive.
        АДАПТИРОВАН ПОД КОНВЕЙЕР (Append mode).
        """
        output_path = f"{self.silver_base_path}/{folder_name}"

        print(f"💾 [1/2] Сохранение данных в {output_path}...")

        # ВАЖНОЕ ИЗМЕНЕНИЕ: используем append, чтобы данные накапливались
        df.write.mode("append").parquet(output_path)
        print("✅ Файлы добавлены.")

        print(f"🏛 [2/2] Проверка регистрации таблицы {table_name}...")
        try:
            # Если таблицы нет в Hive — создаем её.
            # Если есть — ничего не делаем (данные уже лежат в папке, Hive их увидит)
            if not self.spark.catalog.tableExists(table_name):
                print(f"Таблица {table_name} не найдена. Создаем...")
                query = f"""
                    CREATE EXTERNAL TABLE {table_name}
                    USING PARQUET
                    LOCATION '{output_path}'
                """
                self.spark.sql(query)
                print(f"✅ Таблица {table_name} создана.")
            else:
                # Для внешних таблиц Parquet иногда нужно обновить метаданные,
                # но обычно Spark SQL видит новые файлы сразу.
                # На всякий случай можно сделать refresh:
                self.spark.sql(f"REFRESH TABLE {table_name}")
                print(f"✅ Таблица {table_name} обновлена.")

        except Exception as e:
            print(f"❌ Ошибка работы с Hive: {e}")
            raise e

    def process_posts(self):
        print("🚀 Обработка ПОСТОВ...")
        input_path = os.path.join(self.raw_path, "posts")

        try:
            df = self.spark.read.option("multiLine", "true").json(input_path)
        except Exception:
            return

        if not df.head(1): return

        df_clean = df.select(
            col("post_id").cast(IntegerType()).alias("id"),
            col("channel_id").cast(IntegerType()).alias("chat_id"),
            col("date").cast("string"),
            col("text"),
            col("views").cast(IntegerType())
        ).dropDuplicates(['id'])

        df_final = df_clean.withColumn("text", trim(regexp_replace(col("text"), "[\n\r]", " ")))

        self._save_and_register(df_final, "silver_posts", "posts")

    def process_comments(self):
        print("🚀 Обработка КОММЕНТАРИЕВ...")
        input_path = os.path.join(self.raw_path, "comments")

        try:
            df = self.spark.read.option("multiLine", "true").json(input_path)
        except Exception:
            return

        if not df.head(1): return

        # Логика выбора ID
        id_col = "comment_id" if "comment_id" in df.columns else "id"

        df_clean = df.select(
            col(id_col).cast(IntegerType()).alias("id"),
            col("post_id").cast(IntegerType()),
            col("channel_id").cast(IntegerType()).alias("chat_id"),
            col("author_name"),
            col("date").cast("string"),
            col("text")
        ).dropDuplicates(['id'])

        df_final = df_clean.withColumn("text", trim(regexp_replace(col("text"), "[\n\r]", " ")))

        self._save_and_register(df_final, "silver_comments", "comments")

    def run(self):
        self.process_posts()
        self.process_comments()
        self.spark.stop()


if __name__ == "__main__":
    etl = BronzeToSilverETL()
    etl.run()