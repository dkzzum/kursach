import os
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import functions as F

# Настройки стиля (как было в твоих исходниках)
plt.rcParams.update({'figure.figsize': (12, 7), 'figure.dpi': 100})
sns.set_theme(style="whitegrid")

# Добавляем путь к конфигу
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session

REPORT_DIR = "/data/reports"
os.makedirs(REPORT_DIR, exist_ok=True)


def generate_dashboard():
    print("🚀 Генерация отчетов в старом стиле...")

    # Подключаемся к Hive
    spark = get_spark_session("Final_Dashboard_Restored")
    spark.sparkContext.setLogLevel("WARN")

    # =================================================================================
    # 1. ГРАФИК АКТИВНОСТИ (стиль report_generator.py)
    # =================================================================================
    print("\n📈 [1/6] График активности (Posts per Day)...")
    try:
        # Используем silver_posts, так как там есть даты
        # В оригинале было date_ts, у нас date
        df = spark.sql("""
                       SELECT date as post_date, count(*) as post_count
                       FROM gold_posts
                       WHERE date IS NOT NULL
                       GROUP BY date
                       ORDER BY date
                       """).toPandas()

        if not df.empty:
            plt.figure()
            # Стиль из report_generator.py
            sns.lineplot(data=df, x='post_date', y='post_count', marker='o', linewidth=2.5)
            plt.title('Динамика активности канала (Posts per Day)', fontsize=16)
            plt.xlabel('Дата')
            plt.ylabel('Количество постов')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORT_DIR, "1_activity_dynamics.png"))
            print("✅ 1_activity_dynamics.png")
    except Exception as e:
        print(f"❌ Ошибка 1: {e}")

    # =================================================================================
    # 2. ТОП ТОКСИЧНЫХ АВТОРОВ (НОВЫЙ СТИЛЬ - как договаривались)
    # =================================================================================
    print("\n🏆 [2/6] Топ токсичных авторов (Red Style)...")
    try:
        # Берем данные из самой точной модели
        df_top = spark.sql("""
                           SELECT author_name, count(*) as toxic_count
                           FROM gold_bigdata_predictions
                           WHERE prediction = 1.0
                             AND author_name IS NOT NULL
                           GROUP BY author_name
                           ORDER BY toxic_count DESC
                           LIMIT 10
                           """).toPandas()

        plt.figure(figsize=(12, 6))
        # Используем палитру Reds_r для акцента на токсичности
        sns.barplot(data=df_top, x='toxic_count', y='author_name', palette="Reds_r")
        plt.title('Топ-10 авторов деструктивного контента', fontsize=16)
        plt.xlabel('Количество токсичных сообщений')
        plt.ylabel('')
        plt.tight_layout()
        plt.savefig(os.path.join(REPORT_DIR, "2_top_toxic_users.png"))
        print("✅ 2_top_toxic_users.png")
    except Exception as e:
        print(f"❌ Ошибка 2: {e}")

    # =================================================================================
    # 3. ДИАГРАММА СООТНОШЕНИЯ (стиль ml_report.py)
    # =================================================================================
    print("\n🍰 [3/6] Диаграмма токсичности (Pie Chart)...")
    try:
        # Берем самую точную модель (BigData)
        df = spark.sql("""
                       SELECT prediction, count(*) as count
                       FROM gold_bigdata_predictions
                       GROUP BY prediction
                       """).toPandas()

        labels = ['Безопасный контент', 'Деструктивный контент']
        # Проверяем, есть ли оба класса в результатах
        if len(df) == 2:
            df = df.sort_values('prediction')  # Сортируем, чтобы 0.0 было первым
            sizes = df['count'].values
        else:
            # Если нашелся только один класс (редко, но бывает)
            sizes = df['count'].values
            pred_val = int(df['prediction'].iloc[0])
            labels = [labels[pred_val]]

        colors = ['#66b3ff', '#ff9999']  # Цвета из твоего ml_report.py

        plt.figure()
        plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90,
                explode=(0.1, 0) if len(sizes) > 1 else None)
        plt.title('Соотношение деструктивного контента (ML Analysis)', fontsize=14)
        plt.tight_layout()
        plt.savefig(os.path.join(REPORT_DIR, "3_destructive_ratio.png"))
        print("✅ 3_destructive_ratio.png")
    except Exception as e:
        print(f"❌ Ошибка 3: {e}")

    # =================================================================================
    # 4. СРАВНЕНИЕ МЕТОДОВ (стиль final_comparison.py - два пирога)
    # =================================================================================
    print("\n🆚 [4/6] Сравнение методов (Side-by-side Pie)...")
    try:
        def get_stats(table_name):
            try:
                d = spark.sql(f"SELECT prediction, count(*) as count FROM {table_name} GROUP BY prediction").toPandas()
                stats = d.set_index('prediction')['count'].to_dict()
                return stats.get(0.0, 0), stats.get(1.0, 0)
            except:
                return 0, 0

        # Сравниваем самый простой (Словарь) и самый сложный (BigData) методы
        # platinum_destructive создавалась в toxic_classifier.py (словарь)
        safe_rule, toxic_rule = get_stats("platinum_destructive")
        # gold_bigdata_predictions создавалась в train_big_dataset.py
        safe_ml, toxic_ml = get_stats("gold_bigdata_predictions")

        labels = ['Безопасный', 'Деструктивный']
        colors = ['#66b3ff', '#ff9999']

        fig, (ax1, ax2) = plt.subplots(1, 2)

        # График 1: Словарь
        if safe_rule + toxic_rule > 0:
            ax1.pie([safe_rule, toxic_rule], labels=labels, colors=colors, autopct='%1.1f%%', startangle=90,
                    explode=(0, 0.1))
        ax1.set_title('Поиск по словарю\n(Rule-based Approach)')

        # График 2: ML Модель
        if safe_ml + toxic_ml > 0:
            ax2.pie([safe_ml, toxic_ml], labels=labels, colors=colors, autopct='%1.1f%%', startangle=90,
                    explode=(0, 0.1))
        ax2.set_title('Обученная модель\n(Big Data ML)')

        plt.suptitle('Влияние метода анализа на выявление деструктивного контента', fontsize=16)
        plt.savefig(os.path.join(REPORT_DIR, "4_final_comparison.png"))
        print("✅ 4_final_comparison.png")
    except Exception as e:
        print(f"❌ Ошибка 4: {e}")

    # =================================================================================
    # 5. ФИНАЛЬНЫЙ СТОЛБЧАТЫЙ ГРАФИК (стиль mega_report.py)
    # =================================================================================
    print("\n📊 [5/6] Итоговое сравнение % (Bar Chart)...")
    try:
        def get_toxic_percent(table_name):
            try:
                df = spark.read.table(table_name)
                total = df.count()
                toxic = df.filter("prediction = 1.0").count()
                return (toxic / total) * 100 if total > 0 else 0
            except:
                return 0

        # Собираем данные из трех таблиц, которые мы создали на прошлых шагах
        percents = [
            get_toxic_percent("platinum_destructive"),  # Словарь
            get_toxic_percent("gold_supervised_predictions"),  # Средний ML (Supervised)
            get_toxic_percent("gold_bigdata_predictions")  # Big ML
        ]

        methods = [
            "Словарь\n(Rule-based)",
            "ML (Medium)\n(Supervised)",
            "ML (Big Data)\n(FastText)"
        ]

        plt.figure()
        # Цвета из mega_report.py
        bars = plt.bar(methods, percents, color=['#bdc3c7', '#3498db', '#e74c3c'])

        plt.ylabel('Выявлено токсичного контента (%)')
        plt.title('Влияние метода и объема данных на качество анализа', fontsize=14)
        plt.ylim(0, max(percents) + 5)

        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2, yval + 0.5, f"{yval:.1f}%", ha='center', fontweight='bold')

        plt.savefig(os.path.join(REPORT_DIR, "5_final_conclusion.png"))
        print("✅ 5_final_conclusion.png")
    except Exception as e:
        print(f"❌ Ошибка 5: {e}")

    # =================================================================================
    # 6. ГРАФИК МАСШТАБИРУЕМОСТИ (стиль scalability_report.py)
    # =================================================================================
    print("\n🚀 [6/6] График масштабируемости...")
    try:
        # Берем примерные данные из твоих предыдущих логов
        # Small: ~2.5s, Medium: ~5.2s, Big: ~10.3s
        data_sizes = [30000, 326000, 3270000]
        times = [2.5, 5.2, 10.3]

        labels = ["Small\n(30k)", "Medium\n(320k)", "Big Data\n(3.2M)"]

        plt.figure()
        # Синяя линия с маркерами, как в оригинале
        plt.plot(labels, times, marker='o', linestyle='-', color='b', linewidth=2, markersize=8)

        plt.title('Масштабируемость Spark ML (Линейная зависимость)', fontsize=14)
        plt.xlabel('Объем данных')
        plt.ylabel('Время обучения (секунды)')
        plt.grid(True)

        for i, txt in enumerate(times):
            plt.annotate(f"{txt} сек", (labels[i], times[i]), textcoords="offset points", xytext=(0, 10), ha='center')

        plt.savefig(os.path.join(REPORT_DIR, "6_scalability_time.png"))
        print("✅ 6_scalability_time.png")
    except Exception as e:
        print(f"❌ Ошибка 6: {e}")

    spark.stop()
    print(f"\n✨ ВСЕ ОТЧЕТЫ СОХРАНЕНЫ В: {REPORT_DIR}")


if __name__ == "__main__":
    generate_dashboard()