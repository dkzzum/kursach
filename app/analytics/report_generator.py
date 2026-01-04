import os
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Настройки визуального стиля
sns.set_theme(style="whitegrid")
plt.rcParams.update({'figure.figsize': (12, 6), 'figure.dpi': 100})

# 1. Инициализация Spark (с Hive)
spark = SparkSession.builder \
    .appName("TelegramAnalyticsReport") \
    .config("spark.sql.warehouse.dir", "file:///opt/spark/work-dir/spark-warehouse") \
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Папка для сохранения отчетов (внутри контейнера)
REPORT_DIR = "/data/reports"
os.makedirs(REPORT_DIR, exist_ok=True)


def generate_activity_chart():
    print("📈 Генерация графика: Активность по дням...")
    try:
        # SQL: Группируем посты по датам
        df = spark.sql("""
                       SELECT to_date(date_ts) as post_date, count(*) as post_count
                       FROM gold_posts
                       GROUP BY to_date(date_ts)
                       ORDER BY post_date
                       """).toPandas()

        if df.empty:
            print("⚠️ Нет данных для графика активности.")
            return

        # Рисуем линию
        plt.figure()
        sns.lineplot(data=df, x='post_date', y='post_count', marker='o', linewidth=2.5)
        plt.title('Динамика активности канала (Posts per Day)', fontsize=16)
        plt.xlabel('Дата')
        plt.ylabel('Количество постов')
        plt.xticks(rotation=45)
        plt.tight_layout()

        save_path = os.path.join(REPORT_DIR, "1_activity_dynamics.png")
        plt.savefig(save_path)
        print(f"✅ Сохранено: {save_path}")

    except Exception as e:
        print(f"❌ Ошибка графика активности: {e}")


def generate_top_authors_chart():
    print("🏆 Генерация графика: Топ комментаторов...")
    try:
        # SQL: Топ-10 авторов комментов
        df = spark.sql("""
                       SELECT author_name, count(*) as msg_count
                       FROM gold_comments
                       WHERE author_name != 'Hidden/Deleted'
                       GROUP BY author_name
                       ORDER BY msg_count DESC
                       LIMIT 10
                       """).toPandas()

        if df.empty:
            print("⚠️ Нет данных для топа авторов.")
            return

        plt.figure()
        sns.barplot(data=df, x='msg_count', y='author_name', palette='viridis', hue='author_name', legend=False)
        plt.title('Топ-10 самых активных комментаторов', fontsize=16)
        plt.xlabel('Количество комментариев')
        plt.ylabel('')
        plt.tight_layout()

        save_path = os.path.join(REPORT_DIR, "2_top_commentators.png")
        plt.savefig(save_path)
        print(f"✅ Сохранено: {save_path}")

    except Exception as e:
        print(f"❌ Ошибка топа авторов: {e}")


if __name__ == "__main__":
    print("--- 🚀 НАЧАЛО ГЕНЕРАЦИИ ОТЧЕТА ---")
    generate_activity_chart()
    generate_top_authors_chart()
    spark.stop()
    print("--- 🏁 ГОТОВО ---")