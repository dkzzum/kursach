import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.types import IntegerType, StringType


class BronzeToSilverETL:
    def __init__(self):
        # Инициализация Spark с поддержкой Hive
        self.spark = SparkSession.builder \
            .appName("Telegram_Bronze_To_Silver") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .getOrCreate()

        # Пути к данным
        self.raw_path = "/data/raw"

    def process_posts(self):
        """Обработка постов: Raw JSON -> Silver Parquet"""
        print("🚀 Starting processing POSTS...")

        input_path = os.path.join(self.raw_path, "posts")
        # Сохраняем в нашу доступную папку data/silver
        output_path = "/data/silver/posts"

        try:
            df = self.spark.read.option("multiLine", "true").json(input_path)
        except Exception as e:
            print(f"⚠️ Error reading path {input_path}: {e}")
            return

        if not df.head(1):
            print(f"⚠️ No data found in {input_path}")
            return

        # 2. Трансформации
        df_clean = df.select(
            col("post_id").cast(IntegerType()).alias("id"),
            col("channel_id").cast(IntegerType()).alias("chat_id"),
            col("date").cast("string"),
            col("text"),
            col("views").cast(IntegerType())
        )

        df_dedup = df_clean.dropDuplicates(['id'])
        df_final = df_dedup.withColumn("text", trim(regexp_replace(col("text"), "[\n\r]", " ")))

        # 3. Запись в Silver (Parquet)
        print(f"💾 Saving Parquet files to: {output_path}")

        # Это действие сохранило данные на твой жесткий диск
        df_final.write \
            .mode("overwrite") \
            .parquet(output_path)

        print(f"✅ Parquet files saved successfully!")

        # 4. Попытка регистрации в Hive (необязательно для работы)
        try:
            print("🏛 Attempting to register Hive table...")
            self.spark.sql(f"DROP TABLE IF EXISTS silver_posts")
            # Используем SQL синтаксис для внешних таблиц
            self.spark.sql(f"""
                CREATE TABLE silver_posts 
                USING PARQUET 
                LOCATION '{output_path}'
            """)
            print("✅ Hive table registered!")
        except Exception as e:
            print(f"ℹ️ Hive registration skipped (Metadata error), but DATA IS SAVED. Error: {e}")

        print(f"🔥 Processed total: {df_final.count()} posts.")

    def process_comments(self):
        """Обработка комментариев: Raw JSON -> Silver Parquet"""
        print("🚀 Starting processing COMMENTS...")
        input_path = os.path.join(self.raw_path, "comments")
        output_path = "/data/silver/comments"

        try:
            df = self.spark.read.option("multiLine", "true").json(input_path)
        except Exception:
            print(f"⚠️ No comments data found yet in {input_path}")
            return

        if not df.head(1):
            print("⚠️ JSON parsed but data is empty.")
            return

        # ТРАНСФОРМАЦИЯ под твои реальные колонки
        print("🔧 Transforming comments data...")
        df_clean = df.select(
            col("comment_id").cast(IntegerType()).alias("id"),
            col("post_id").cast(IntegerType()),
            col("channel_id").cast(IntegerType()).alias("chat_id"),
            col("author_name"),
            col("date").cast("string"),
            col("text")
        )

        # Удаляем дубликаты по id комментария
        df_dedup = df_clean.dropDuplicates(['id'])

        # Очистка текста от переносов строк
        df_final = df_dedup.withColumn("text", trim(regexp_replace(col("text"), "[\n\r]", " ")))

        # ЗАПИСЬ
        print(f"💾 Saving Parquet comments to: {output_path}")
        df_final.write.mode("overwrite").parquet(output_path)
        print(f"✅ Comments Parquet saved successfully!")

        # РЕГИСТРАЦИЯ В HIVE (пробуем, но не боимся ошибок)
        try:
            self.spark.sql(f"DROP TABLE IF EXISTS silver_comments")
            self.spark.sql(f"CREATE TABLE silver_comments USING PARQUET LOCATION '{output_path}'")
            print("✅ Hive table 'silver_comments' registered!")
        except Exception:
            print("ℹ️ Hive registration skipped, but DATA IS SAVED.")

        print(f"🔥 Processed total: {df_final.count()} comments.")

    def run(self):
        self.process_posts()
        self.process_comments() # Раскомментируй, когда будут комментарии
        self.spark.stop()


if __name__ == "__main__":
    etl = BronzeToSilverETL()
    etl.run()