import os
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pyspark.sql import SparkSession

# Настройки графиков
plt.rcParams.update({'figure.figsize': (10, 6), 'figure.dpi': 100})
sns.set_theme(style="whitegrid")

# Инициализация Spark
spark = SparkSession.builder \
    .appName("ML_Reporting") \
    .config("spark.sql.warehouse.dir", "file:///opt/spark/work-dir/spark-warehouse") \
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby") \
    .enableHiveSupport() \
    .getOrCreate()

REPORT_DIR = "/data/reports"
os.makedirs(REPORT_DIR, exist_ok=True)


def draw_toxicity_pie_chart():
    print("🎨 Рисуем диаграмму токсичности...")

    # Считаем кол-во безопасных (0.0) и токсичных (1.0)
    df = spark.sql("""
                   SELECT prediction, count(*) as count
                   FROM platinum_destructive
                   GROUP BY prediction
                   """).toPandas()

    # Красивые подписи
    labels = ['Безопасный контент', 'Деструктивный контент']
    # Если вдруг модель нашла только один класс, обработаем это
    if len(df) == 2:
        sizes = df.sort_values('prediction')['count'].values
    else:
        sizes = df['count'].values
        labels = [labels[int(df['prediction'][0])]]

    # Цвета (Зеленый / Красный)
    colors = ['#66b3ff', '#ff9999']

    plt.figure()
    plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90, explode=(0.1, 0))
    plt.title('Соотношение деструктивного контента (ML Analysis)')
    plt.tight_layout()

    save_path = os.path.join(REPORT_DIR, "3_destructive_ratio.png")
    plt.savefig(save_path)
    print(f"✅ График сохранен: {save_path}")


if __name__ == "__main__":
    draw_toxicity_pie_chart()
    spark.stop()