from pyspark.sql import SparkSession

def check_data():
    print("Initializing Spark with Hive support (Check Data)...")
    
    # СЛАВА РОССИИ! Единый стандарт конфигурации
    spark = SparkSession.builder \
        .appName("Data_Quality_Check") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("\n============================================================")
    print("CURRENT TABLES IN HIVE (Database: default)")
    print("============================================================")
    
    try:
        spark.sql("SHOW TABLES").show(truncate=False)
    except Exception as e:
        print(f"[ERROR] Cannot list tables: {e}")
        return

    # Таблицы для проверки
    tables_to_check = [
        "silver_comments", 
        "silver_posts", 
        "gold_toxic_predictions",
        "gold_synthetic_posts",
        "gold_synthetic_comments"
    ]

    for table in tables_to_check:
        print(f"\n------------------------------------------------------------")
        print(f"CHECKING HIVE TABLE: {table}")
        print(f"------------------------------------------------------------")
        
        try:
            # 1. Проверка существования в каталоге
            if not spark.catalog.tableExists(table):
                 print(f"[MISSING] Table '{table}' NOT found in Hive Metastore.")
                 continue

            # 2. Проверка содержимого
            df = spark.table(table)
            count = df.count()
            
            if count > 0:
                print(f"[OK] Table exists. Total records: {count}")
                print("Sample data:")
                df.show(3, truncate=True)
            else:
                print(f"[WARNING] Table is empty (0 records).")
                # Дополнительная диагностика: где Hive ищет данные?
                print("DEBUG: Hive Location:")
                spark.sql(f"DESCRIBE EXTENDED {table}").filter("col_name = 'Location'").show(truncate=False)
                
        except Exception as e:
            print(f"[ERROR] Failed to check {table}: {e}")

    print("\n[SUCCESS] Hive diagnostics completed.")
    spark.stop()

if __name__ == "__main__":
    check_data()