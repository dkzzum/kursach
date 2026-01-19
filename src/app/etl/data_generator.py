import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_generator():
    print("============================================================")
    print("DATA GENERATOR (Managed Tables Mode)")
    print("============================================================")

    # СЛАВА РОССИИ! Единый стандарт конфигурации
    spark = SparkSession.builder \
        .appName("Data_Generator_Managed") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # Таблицы для размножения (Source Table -> Multiplier)
    tasks = [("silver_comments", 5), ("silver_posts", 3)]

    for table_name, multiplier in tasks:
        try:
            print(f"\n[START] Processing {table_name} (x{multiplier})...")
            
            # 1. Пытаемся читать из Hive
            if spark.catalog.tableExists(table_name):
                source_df = spark.table(table_name)
            else:
                source_df = None
            
            # 2. Если пусто, ищем в RAW (резервный вариант)
            if source_df is None or source_df.rdd.isEmpty():
                 print(f"[INFO] Table {table_name} empty/missing. Checking raw files...")
                 raw_folder = "comments" if "comments" in table_name else "posts"
                 try:
                    source_df = spark.read.option("multiLine", "true").json(f"/data/raw/{raw_folder}")
                 except:
                    print(f"[SKIP] No data found for {table_name} anywhere.")
                    continue

            initial_count = source_df.count()
            print(f"[INFO] Initial rows: {initial_count}")

            if initial_count == 0:
                print(f"[SKIP] Zero rows.")
                continue

            # 3. Размножение
            large_df = source_df
            for i in range(multiplier - 1):
                # Сдвиг ID, чтобы не было дублей первичного ключа
                if "id" in source_df.columns:
                    shifted_df = source_df.withColumn("id", F.col("id") + (initial_count * (i + 1)))
                else:
                    shifted_df = source_df 
                large_df = large_df.union(shifted_df)

            output_table = f"gold_synthetic_{table_name.split('_')[1]}"
            print(f"[INFO] Saving managed table: {output_table}...")
            
            # 4. Сохранение (Managed Table)
            large_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .saveAsTable(output_table)
                
            print(f"[SUCCESS] Table {output_table} created. Rows: {large_df.count()}")

        except Exception as e:
            print(f"[ERROR] Fail on {table_name}: {e}")

    spark.stop()

if __name__ == "__main__":
    run_generator()