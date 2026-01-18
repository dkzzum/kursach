import os
import sys
import shutil
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# Добавляем путь к корню для импорта конфига
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session

def fix_path(path):
    """Исправляет пути для Windows Git Bash и Linux"""
    normalized = os.path.normpath(path).replace('\\', '/')
    if normalized.startswith('//'):
        normalized = '/' + normalized.lstrip('/')
    return normalized

def register_table_sql(spark, table_name, path, schema_df):
    """
    Регистрирует таблицу в Hive через чистый SQL.
    Убрана команда MSCK REPAIR, так как таблица не партиционирована.
    """
    print(f"[INFO] Registering Hive table: {table_name}")
    
    # Формируем строку схемы (col_name TYPE, col_name TYPE)
    schema_parts = []
    for field in schema_df.schema:
        dtype = field.dataType.simpleString()
        schema_parts.append(f"{field.name} {dtype}")
    schema_ddl = ", ".join(schema_parts)

    try:
        # 1. Удаляем старую ссылку
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        
        # 2. Создаем новую внешнюю таблицу
        # Поскольку мы указываем LOCATION, Hive сразу увидит данные
        sql = f"""
            CREATE EXTERNAL TABLE {table_name} (
                {schema_ddl}
            )
            STORED AS PARQUET
            LOCATION '{path}'
        """
        spark.sql(sql)
        
        # 3. Проверка
        count = spark.table(table_name).count()
        print(f"[SUCCESS] Table '{table_name}' registered in Hive. Row count: {count}")
        
    except Exception as e:
        print(f"[ERROR] Could not register Hive table {table_name}: {e}")

def process_multiplication(spark, source_path, target_path, target_table_name, multiplier):
    print(f"\n[START] Processing {target_table_name} (x{multiplier})...")
    
    clean_source = fix_path(source_path)
    clean_target = fix_path(target_path)

    # 1. Читаем исходные данные (Silver)
    if not os.path.exists(clean_source):
        print(f"[ERROR] Source path does not exist: {clean_source}")
        return

    df_source = spark.read.parquet(clean_source)
    initial_count = df_source.count()
    print(f"[INFO] Initial rows: {initial_count}")

    if initial_count == 0:
        print("[WARN] Source is empty, skipping.")
        return

    # 2. Умножаем данные
    df_multiplier = spark.range(multiplier).withColumnRenamed("id", "idx")
    df_multiplied = df_source.crossJoin(df_multiplier)

    # 3. Уникализируем данные (сдвигаем даты)
    cols = df_source.columns
    date_col = None
    if "date_ts" in cols: date_col = "date_ts"
    elif "date" in cols: date_col = "date"

    if date_col:
        print(f"[INFO] Shifting dates in column '{date_col}' to verify uniqueness...")
        df_final = df_multiplied.withColumn(
            date_col, 
            F.date_sub(F.col(date_col).cast(DateType()), F.col("idx").cast("int"))
        )
    else:
        df_final = df_multiplied
    
    # Убираем служебную колонку и добавляем флаг синтетики
    df_final = df_final.drop("idx").withColumn("is_synthetic", F.lit(True))

    expected_count = initial_count * multiplier
    print(f"[INFO] Expected rows after generation: {expected_count}")

    # 4. Сохраняем физически (Gold)
    print(f"[INFO] Saving Parquet to: {clean_target}")
    if os.path.exists(clean_target):
        shutil.rmtree(clean_target, ignore_errors=True)
    
    df_final.write.mode("overwrite").parquet(clean_target)

    # 5. Регистрируем в Hive
    register_table_sql(spark, target_table_name, clean_target, df_final)

def main():
    print("=" * 60)
    print("DATA GENERATOR (Parquet -> Hive Table)")
    print("=" * 60)

    spark = get_spark_session("HiveDataGenerator")
    spark.sparkContext.setLogLevel("ERROR")

    # --- КОНФИГУРАЦИЯ ---
    

    process_multiplication(
        spark,
        source_path="/data/silver/comments",
        target_path="/data/gold/synthetic_comments",
        target_table_name="gold_synthetic_comments",
        multiplier=5
    )

    print("\n[DONE] Generation finished.")
    spark.stop()

if __name__ == "__main__":
    main()