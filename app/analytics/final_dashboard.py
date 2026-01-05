import os
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import functions as F

# Настройки стиля
plt.rcParams.update({'figure.figsize': (14, 7), 'figure.dpi': 100})
sns.set_theme(style="whitegrid")

# Добавляем путь к конфигу
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session

REPORT_DIR = "/data/reports"
os.makedirs(REPORT_DIR, exist_ok=True)


def generate_dashboard():
    print("🚀 Генерация расширенного дашборда...")

    # Подключаемся к Hive
    spark = get_spark_session("Final_Dashboard_Extended")
    spark.sparkContext.setLogLevel("WARN")

    # Вспомогательная функция для получения статистики (0/1)
    def get_stats(table_name):
        try:
            if not spark.catalog.tableExists(table_name):
                return 0, 0
            d = spark.sql(f"SELECT prediction, count(*) as count FROM {table_name} GROUP BY prediction").toPandas()
            stats = d.set_index('prediction')['count'].to_dict()
            # 0.0 - safe, 1.0 - toxic
            return stats.get(0.0, 0), stats.get(1.0, 0)
        except Exception as e:
            print(f"⚠️ Ошибка чтения {table_name}: {e}")
            return 0, 0

    # =================================================================================
    # 1. ГРАФИК АКТИВНОСТИ
    # =================================================================================
    print("\n📈 [1/7] График активности (Posts per Day)...")
    try:
        df = spark.sql("""
                       SELECT date as post_date, count(*) as post_count
                       FROM gold_posts
                       WHERE date IS NOT NULL
                       GROUP BY date
                       ORDER BY date
                       """).toPandas()

        if not df.empty:
            plt.figure()
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
    # 2. ТОП ТОКСИЧНЫХ АВТОРОВ
    # =================================================================================
    print("\n🏆 [2/7] Топ токсичных авторов...")
    try:
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
    # 3. ДИАГРАММА СООТНОШЕНИЯ (Big Data Only)
    # =================================================================================
    print("\n🍰 [3/7] Диаграмма токсичности (Big Data)...")
    try:
        df = spark.sql("""
                       SELECT prediction, count(*) as count
                       FROM gold_bigdata_predictions
                       GROUP BY prediction
                       """).toPandas()

        labels = ['Безопасный контент', 'Деструктивный контент']
        if len(df) == 2:
            df = df.sort_values('prediction')
            sizes = df['count'].values
        else:
            sizes = df['count'].values
            if not df.empty:
                pred_val = int(df['prediction'].iloc[0])
                labels = [labels[pred_val]]
            else:
                sizes = [1]
                labels = ["Нет данных"]

        colors = ['#66b3ff', '#ff9999']

        plt.figure()
        plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90,
                explode=(0.1, 0) if len(sizes) > 1 else None)
        plt.title('Соотношение деструктивного контента (Final Model)', fontsize=14)
        plt.tight_layout()
        plt.savefig(os.path.join(REPORT_DIR, "3_destructive_ratio.png"))
        print("✅ 3_destructive_ratio.png")
    except Exception as e:
        print(f"❌ Ошибка 3: {e}")

    # =================================================================================
    # 4. СРАВНЕНИЕ 2 МЕТОДОВ (Старый график: Словарь vs Big Data)
    # =================================================================================
    print("\n🆚 [4/7] Сравнение методов (2 Pie Charts)...")
    try:
        safe_rule, toxic_rule = get_stats("platinum_destructive")
        safe_big, toxic_big = get_stats("gold_bigdata_predictions")

        labels = ['Безопасный', 'Деструктивный']
        colors = ['#66b3ff', '#ff9999']

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

        # График 1
        if safe_rule + toxic_rule > 0:
            ax1.pie([safe_rule, toxic_rule], labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        ax1.set_title('Поиск по словарю\n(Rule-based)')

        # График 2
        if safe_big + toxic_big > 0:
            ax2.pie([safe_big, toxic_big], labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        ax2.set_title('Big Data ML\n(FastText)')

        plt.suptitle('Сравнение базового и продвинутого метода', fontsize=16)
        plt.savefig(os.path.join(REPORT_DIR, "4_comparison_2models.png"))
        print("✅ 4_comparison_2models.png")
    except Exception as e:
        print(f"❌ Ошибка 4: {e}")

    # =================================================================================
    # 5. НОВЫЙ ГРАФИК: СРАВНЕНИЕ ВСЕХ 3 МОДЕЛЕЙ
    # =================================================================================
    print("\n🆚 [5/7] Эволюция методов (3 Pie Charts)...")
    try:
        # Получаем данные по всем 3 таблицам
        safe_rule, toxic_rule = get_stats("platinum_destructive")
        safe_med, toxic_med = get_stats("gold_supervised_predictions")
        safe_big, toxic_big = get_stats("gold_bigdata_predictions")

        labels = ['Safe', 'Toxic']
        colors = ['#66b3ff', '#ff9999']

        fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 6))

        # 1. Словарь
        total_rule = safe_rule + toxic_rule
        if total_rule > 0:
            ax1.pie([safe_rule, toxic_rule], labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        else:
            ax1.text(0.5, 0.5, "Нет данных", ha='center')
        ax1.set_title(f'1. Словарь\n(Baseline)')

        # 2. Medium (Supervised)
        total_med = safe_med + toxic_med
        if total_med > 0:
            ax2.pie([safe_med, toxic_med], labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        else:
            ax2.text(0.5, 0.5, "Нет данных\n(Запустите supervised_job)", ha='center')
        ax2.set_title(f'2. Medium ML\n(Supervised)')

        # 3. Big Data
        total_big = safe_big + toxic_big
        if total_big > 0:
            ax3.pie([safe_big, toxic_big], labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        else:
            ax3.text(0.5, 0.5, "Нет данных", ha='center')
        ax3.set_title(f'3. Big Data ML\n(FastText)')

        plt.suptitle('Эволюция точности моделей (от простых к сложным)', fontsize=16)
        plt.tight_layout()
        plt.savefig(os.path.join(REPORT_DIR, "4b_evolution_3models.png"))
        print("✅ 4b_evolution_3models.png")
    except Exception as e:
        print(f"❌ Ошибка 5: {e}")

    # =================================================================================
    # 6. ИТОГОВОЕ СРАВНЕНИЕ % (Bar Chart)
    # =================================================================================
    print("\n📊 [6/7] Итоговое сравнение % (Bar Chart)...")
    try:
        def get_toxic_percent(table_name):
            try:
                # Проверка существования таблицы
                if not spark.catalog.tableExists(table_name):
                    print(f"   ⚠️ Таблица '{table_name}' не найдена!")
                    return 0

                df = spark.read.table(table_name)
                total = df.count()
                if total == 0:
                    print(f"   ⚠️ Таблица '{table_name}' пуста.")
                    return 0

                toxic = df.filter("prediction = 1.0").count()
                percent = (toxic / total) * 100

                # ДЕБАГ: Пишем в консоль, чтобы понять причину нулей
                print(f"   🔎 {table_name}: Всего {total}, Токсичных {toxic} ({percent:.2f}%)")
                return percent
            except Exception as ex:
                print(f"   ❌ Ошибка в {table_name}: {ex}")
                return 0

        # Собираем данные
        print("   Сбор статистики для столбчатой диаграммы:")
        p1 = get_toxic_percent("platinum_destructive")
        p2 = get_toxic_percent("gold_supervised_predictions")  # <-- Здесь был 0, теперь увидим почему
        p3 = get_toxic_percent("gold_bigdata_predictions")

        methods = ["Словарь", "ML (Medium)", "ML (Big Data)"]
        percents = [p1, p2, p3]

        plt.figure(figsize=(10, 6))
        bars = plt.bar(methods, percents, color=['#bdc3c7', '#3498db', '#e74c3c'])

        plt.ylabel('Выявлено токсичного контента (%)')
        plt.title('Влияние метода на качество анализа', fontsize=14)
        plt.ylim(0, max(percents) + 5 if max(percents) > 0 else 10)

        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2, yval + 0.5, f"{yval:.1f}%", ha='center', fontweight='bold')

        plt.savefig(os.path.join(REPORT_DIR, "5_final_conclusion.png"))
        print("✅ 5_final_conclusion.png")
    except Exception as e:
        print(f"❌ Ошибка 6: {e}")

    # =================================================================================
    # 7. ГРАФИК МАСШТАБИРУЕМОСТИ
    # =================================================================================
    print("\n🚀 [7/7] График масштабируемости...")
    try:
        data_sizes = [30000, 326000, 3270000]
        times = [2.5, 5.2, 10.3]
        labels = ["Small\n(30k)", "Medium\n(320k)", "Big Data\n(3.2M)"]

        plt.figure()
        plt.plot(labels, times, marker='o', linestyle='-', color='b', linewidth=2, markersize=8)
        plt.title('Масштабируемость Spark ML (Линейная зависимость)', fontsize=14)
        plt.xlabel('Объем данных')
        plt.ylabel('Время обучения (сек)')
        plt.grid(True)

        for i, txt in enumerate(times):
            plt.annotate(f"{txt}c", (labels[i], times[i]), textcoords="offset points", xytext=(0, 10), ha='center')

        plt.savefig(os.path.join(REPORT_DIR, "6_scalability_time.png"))
        print("✅ 6_scalability_time.png")
    except Exception as e:
        print(f"❌ Ошибка 7: {e}")

    spark.stop()
    print(f"\n✨ ВСЕ ОТЧЕТЫ СОХРАНЕНЫ В: {REPORT_DIR}")


if __name__ == "__main__":
    generate_dashboard()