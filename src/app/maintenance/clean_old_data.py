import os
import time
import shutil
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date, date_sub, lit


class DataCleaner:
    def __init__(self):
        self.RETENTION_DAYS = 7
        self.RAW_DIR = "/data/raw"

        # Настраиваем Spark для работы с Hive
        self.spark = SparkSession.builder \
            .appName("Data_Cleaner_Job") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def clean_filesystem_raw(self):
        """Удаляет старые JSON файлы из папки Raw"""
        print(f"[FS] Очистка папки {self.RAW_DIR} (старше {self.RETENTION_DAYS} дн)...")

        deleted_count = 0
        now = time.time()
        cutoff = self.RETENTION_DAYS * 86400  # Дни в секунды

        for root, dirs, files in os.walk(self.RAW_DIR):
            for fname in files:
                fpath = os.path.join(root, fname)
                try:
                    # Дата модификации файла
                    mtime = os.path.getmtime(fpath)
                    if (now - mtime) > cutoff:
                        os.remove(fpath)
                        deleted_count += 1
                except Exception as e:
                    print(f"Ошибка при удалении {fpath}: {e}")

        print(f"[FS] Удалено {deleted_count} старых файлов.")

    def clean_hive_table(self, table_name, date_col="date"):
        """Удаляет старые записи из таблицы Hive"""
        print(f"[HIVE] Очистка таблицы {table_name}...")

        if not self.spark.catalog.tableExists(table_name):
            print(f"Таблица {table_name} не найдена, пропускаем.")
            return

        # 1. Читаем таблицу
        df = self.spark.table(table_name)
        initial_count = df.count()

        # 2. Фильтруем (Оставляем только СВЕЖИЕ данные)
        df_clean = df.filter(
            to_date(col(date_col)) >= date_sub(current_date(), self.RETENTION_DAYS)
        )

        final_count = df_clean.count()
        deleted_rows = initial_count - final_count

        if deleted_rows > 0:
            print(f"Обнаружено {deleted_rows} старых записей. Перезаписываем таблицу...")
            # Перезаписываем таблицу только свежими данными
            df_clean.write \
                .mode("overwrite") \
                .saveAsTable(table_name)
            print(f"[HIVE] Таблица {table_name} обновлена. Текущий размер: {final_count}")
        else:
            print(f"[HIVE] Таблица {table_name} чиста (нет данных старше {self.RETENTION_DAYS} дн).")

    def run(self):
        # 1. Чистим файлы
        self.clean_filesystem_raw()

        # 2. Чистим таблицы (Silver)
        self.clean_hive_table("silver_comments", date_col="date")
        # Если есть таблица постов, можно добавить и её
        # self.clean_hive_table("silver_posts", date_col="date")

        self.spark.stop()


if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run()