import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

# Настройки графиков
plt.rcParams.update({'figure.figsize': (12, 8), 'figure.dpi': 100})
sns.set_theme(style="whitegrid")
REPORT_DIR = "/data/reports"

spark = SparkSession.builder \
    .appName("Final_Mega_Report") \
    .config("spark.sql.warehouse.dir", "file:///opt/spark/work-dir/spark-warehouse") \
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby") \
    .enableHiveSupport() \
    .getOrCreate()


def get_toxic_percent(table_name, method_name):
    print(f"📊 Анализ таблицы {table_name} ({method_name})...")
    try:
        # Считаем процент токсичных сообщений (prediction = 1.0)
        df = spark.sql(f"SELECT prediction, count(*) as cnt FROM {table_name} GROUP BY prediction").toPandas()

        total = df['cnt'].sum()
        toxic_count = df[df['prediction'] == 1.0]['cnt'].sum()

        percent = (toxic_count / total) * 100
        print(f"   -> Найдено {toxic_count} токсичных ({percent:.2f}%)")
        return percent
    except Exception as e:
        print(f"   ⚠️ Ошибка (возможно таблицы нет): {e}")
        return 0


def draw_final_chart():
    # Собираем данные из трех таблиц
    methods = [
        "Словарь\n(Rule-based)",
        "ML (14k примеров)\n(Small Data)",
        "ML (250k примеров)\n(Big Data)"
    ]

    # Внимание: убедитесь, что таблицы существуют!
    # Если вы какую-то пропустили, там будет 0
    percents = [
        get_toxic_percent("platinum_destructive", "Dictionary"),
        get_toxic_percent("platinum_supervised", "Small ML"),
        get_toxic_percent("platinum_bigdata", "Big ML")
    ]

    # Рисуем график
    plt.figure()
    bars = plt.bar(methods, percents, color=['#bdc3c7', '#3498db', '#e74c3c'])

    plt.ylabel('Выявлено токсичного контента (%)')
    plt.title('Влияние метода и объема данных на качество анализа', fontsize=14)
    plt.ylim(0, max(percents) + 5)  # Чуть выше самого большого столбца

    # Подписи над столбцами
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, yval + 0.5, f"{yval:.1f}%", ha='center', fontweight='bold')

    # Сохраняем
    save_path = os.path.join(REPORT_DIR, "5_final_conclusion.png")
    plt.savefig(save_path)
    print(f"\n✅ ФИНАЛЬНЫЙ ГРАФИК ГОТОВ: {save_path}")


if __name__ == "__main__":
    draw_final_chart()
    spark.stop()