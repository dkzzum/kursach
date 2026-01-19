import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.types import IntegerType

class BronzeToSilverETL:
    def __init__(self):
        print("[INFO] Инициализация Spark Session (ФИНАЛ)...")
        self.warehouse_path = "/opt/hive/data/warehouse"
        
        self.spark = SparkSession.builder \
            .appName("Telegram_Bronze_To_Silver") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.warehouse.dir", self.warehouse_path) \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .enableHiveSupport() \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")
        self.raw_path = "/data/raw"

    def _save_as_table(self, df, table_name):
        target_path = f"{self.warehouse_path}/{table_name}"
        
        if df.count() == 0:
            print(f"{table_name}: Пустой датафрейм, пропускаем.")
            return

        print(f"[INFO] Пишем {df.count()} строк в {target_path}...")
        
        # 1. Пишем Parquet напрямую (игнорируя Hive)
        df.write.mode("overwrite").parquet(target_path)
            
        # 2. Регистрируем таблицу поверх файлов
        print(f"[INFO] Регистрируем таблицу {table_name}...")
        self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        self.spark.sql(f"CREATE TABLE {table_name} USING parquet LOCATION '{target_path}'")
        self.spark.sql(f"REFRESH TABLE {table_name}")
        
        print(f"УСПЕХ: {table_name} готова!")

    def process_data(self):
        print("🔨 Начинаем обработку...")
        
        # --- POSTS ---
        try:
            posts_path = os.path.join(self.raw_path, "posts")
            df_posts = self.spark.read.option("multiLine", "true").option("recursiveFileLookup", "true").json(posts_path)
            
            if "post_id" in df_posts.columns:
                clean_posts = df_posts.select(
                    col("post_id").cast(IntegerType()).alias("id"),
                    col("channel_id").cast(IntegerType()).alias("chat_id"),
                    col("date").cast("string"),
                    col("text"),
                    col("views").cast(IntegerType())
                ).dropDuplicates(['id'])
                
                final_posts = clean_posts.withColumn("text", trim(regexp_replace(col("text"), "[\\n\\r]", " ")))
                self._save_as_table(final_posts, "silver_posts")
        except Exception as e:
            print(f"Ошибка Posts: {e}")

        # --- COMMENTS ---
        try:
            comments_path = os.path.join(self.raw_path, "comments")
            df_comm = self.spark.read.option("multiLine", "true").option("recursiveFileLookup", "true").json(comments_path)
            
            id_col = "comment_id" if "comment_id" in df_comm.columns else "id"
            if id_col in df_comm.columns:
                clean_comm = df_comm.select(
                    col(id_col).cast(IntegerType()).alias("id"),
                    col("post_id").cast(IntegerType()),
                    col("channel_id").cast(IntegerType()).alias("chat_id"),
                    col("author_name"),
                    col("date").cast("string"),
                    col("text")
                ).dropDuplicates(['id'])
                
                final_comm = clean_comm.withColumn("text", trim(regexp_replace(col("text"), "[\\n\\r]", " ")))
                self._save_as_table(final_comm, "silver_comments")
        except Exception as e:
            print(f"Ошибка Comments: {e}")

    def run(self):
        self.process_data()
        self.spark.stop()
        print("\nЗАДАЧА ВЫПОЛНЕНА. СЛАВА РОССИИ!")

if __name__ == "__main__":
    BronzeToSilverETL().run()