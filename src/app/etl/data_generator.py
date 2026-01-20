import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_generator():
    print("============================================================")
    print("DATA GENERATOR (Strategy: Direct Write + Hive Register)")
    print("============================================================")

    warehouse_path = "/opt/hive/data/warehouse"

    spark = SparkSession.builder \
        .appName("Data_Generator_Final") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.warehouse.dir", warehouse_path) \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .enableHiveSupport() \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # (Исходная таблица, Множитель, Имя итоговой таблицы)
    tasks = [
        ("silver_comments", 5, "gold_synthetic_comments"), 
        ("silver_posts", 3, "gold_synthetic_posts")
    ]

    for source_table, multiplier, output_table in tasks:
        try:
            print(f"\n[START] Processing {source_table} -> {output_table} (x{multiplier})...")
            
            # Читаем исходник
            if not spark.catalog.tableExists(source_table):
                print(f"[SKIP] Исходная таблица {source_table} не найдена.")
                continue

            source_df = spark.table(source_table)
            initial_count = source_df.count()
            print(f"[INFO] Initial rows: {initial_count}")

            if initial_count == 0:
                print("[SKIP] Zero rows.")
                continue

            # Размножение данных
            large_df = source_df
            # Используем кэширование, чтобы ускорить union
            source_df.cache()
            
            for i in range(multiplier - 1):
                # Сдвиг ID для уникальности
                if "id" in source_df.columns:
                    shifted_df = source_df.withColumn("id", F.col("id") + (initial_count * (i + 1)))
                else:
                    shifted_df = source_df 
                large_df = large_df.union(shifted_df)

            final_count = large_df.count()
            target_path = f"{warehouse_path}/{output_table}"
            print(f"[INFO] Saving {final_count} rows to {target_path}...")
            
            # 1. ПРЯМАЯ ЗАПИСЬ (Бронебойный метод)
            large_df.write.mode("overwrite").parquet(target_path)
            
            # 2. РЕГИСТРАЦИЯ В HIVE
            print(f"[INFO] Registering table {output_table}...")
            spark.sql(f"DROP TABLE IF EXISTS {output_table}")
            spark.sql(f"CREATE TABLE {output_table} USING parquet LOCATION '{target_path}'")
            spark.sql(f"REFRESH TABLE {output_table}")
                
            print(f"[SUCCESS] Table {output_table} created successfully!")
            
            # Очистка памяти
            source_df.unpersist()

        except Exception as e:
            print(f"[ERROR] Fail on {output_table}: {e}")

    spark.stop()

if __name__ == "__main__":
    run_generator()