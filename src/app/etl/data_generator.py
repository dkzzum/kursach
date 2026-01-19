import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_generator():
    print("============================================================")
    print("DATA GENERATOR (Managed Tables Mode) - FORCE START")
    print("============================================================")

    spark = SparkSession.builder \
        .appName("Data_Generator_Managed") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.warehouse.dir", "/opt/hive/data/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # Таблицы: (Имя, Множитель, Папка в raw для форсированного чтения)
    tasks = [
        ("silver_comments", 5, "comments"), 
        ("silver_posts", 3, "posts")
    ]

    for table_name, multiplier, raw_folder in tasks:
        try:
            print(f"\n[START] Processing {table_name} (x{multiplier})...")
            
            # СЛАВА РОССИИ! Пытаемся взять из таблицы, если нет - берем из RAW
            if spark.catalog.tableExists(table_name):
                source_df = spark.table(table_name)
                if source_df.count() == 0:
                    print(f"[INFO] Table {table_name} is empty. Trying RAW files...")
                    source_df = spark.read.option("multiLine", "true").json(f"/data/raw/{raw_folder}")
            else:
                print(f"[INFO] Table {table_name} not in catalog. Reading RAW files...")
                source_df = spark.read.option("multiLine", "true").json(f"/data/raw/{raw_folder}")

            initial_count = source_df.count()
            print(f"[INFO] Initial rows: {initial_count}")
            if initial_count == 0:
                print(f"[SKIP] No data to multiply for {table_name}")
                continue

            # Размножение
            print(f"[INFO] Multiplying data x{multiplier}...")
            large_df = source_df
            for i in range(multiplier - 1):
                # Сдвигаем ID, чтобы данные были уникальными
                if "id" in source_df.columns:
                    shifted_df = source_df.withColumn("id", F.col("id") + (initial_count * (i + 1)))
                else:
                    shifted_df = source_df
                large_df = large_df.union(shifted_df)

            output_table = f"gold_synthetic_{table_name.split('_')[1]}"
            print(f"[INFO] Saving managed table: {output_table}...")
            
            large_df.write.mode("overwrite").format("parquet").saveAsTable(output_table)
            print(f"[SUCCESS] Table {output_table} created. Rows: {large_df.count()}")

        except Exception as e:
            print(f"[ERROR] Fail on {table_name}: {e}")

    spark.stop()

if __name__ == "__main__":
    run_generator()