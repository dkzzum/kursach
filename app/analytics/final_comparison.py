import os
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pyspark.sql import SparkSession

# Настройки
plt.rcParams.update({'figure.figsize': (14, 7), 'figure.dpi': 100})
sns.set_theme(style="whitegrid")
REPORT_DIR = "/data/reports"

spark = SparkSession.builder \
    .appName("Final_Comparison_Report") \
    .config("spark.sql.warehouse.dir", "file:///opt/spark/work-dir/spark-warehouse") \
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby") \
    .enableHiveSupport() \
    .getOrCreate()


def get_stats(table_name):
    try:
        df = spark.sql(f"SELECT prediction, count(*) as count FROM {table_name} GROUP BY prediction").toPandas()
        # Превращаем в словарь {0.0: count, 1.0: count}
        stats = df.set_index('prediction')['count'].to_dict()
        return stats.get(0.0, 0), stats.get(1.0, 0)  # safe, toxic
    except:
        return 0, 0


def draw_comparison():
    print("🎨 Рисуем финальное сравнение методов...")

    # Получаем данные
    safe_rule, toxic_rule = get_stats("platinum_destructive")
    safe_ml, toxic_ml = get_stats("platinum_supervised")

    # Данные для графиков
    labels = ['Безопасный', 'Деструктивный']
    colors = ['#66b3ff', '#ff9999']  # Голубой и Красный

    fig, (ax1, ax2) = plt.subplots(1, 2)

    # График 1: Словарь
    ax1.pie([safe_rule, toxic_rule], labels=labels, colors=colors, autopct='%1.1f%%', startangle=90, explode=(0, 0.1))
    ax1.set_title(f'Поиск по словарю\n(Rule-based Approach)')

    # График 2: ML Модель
    ax2.pie([safe_ml, toxic_ml], labels=labels, colors=colors, autopct='%1.1f%%', startangle=90, explode=(0, 0.1))
    ax2.set_title(f'Обученная модель\n(Supervised ML)')

    plt.suptitle('Влияние метода анализа на выявление деструктивного контента', fontsize=16)

    save_path = os.path.join(REPORT_DIR, "4_final_comparison.png")
    plt.savefig(save_path)
    print(f"✅ Финальный график сохранен: {save_path}")


if __name__ == "__main__":
    draw_comparison()
    spark.stop()