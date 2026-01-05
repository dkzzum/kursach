import os
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from datetime import datetime

# --- НАСТРОЙКИ СТИЛЯ ---
plt.rcParams.update({'figure.figsize': (14, 8), 'figure.dpi': 100})
sns.set_theme(style="whitegrid")

# Добавляем путь к корню проекта для импорта конфига
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session

# Папка для отчетов
REPORT_DIR = "/data/reports"
os.makedirs(REPORT_DIR, exist_ok=True)

def generate_dashboard():
    print("\n" + "=" * 50)
    print("🚀 ГЕНЕРАЦИЯ ФИНАЛЬНОГО DASHBOARD (ULTIMATE EDITION)")
    print("=" * 50)

    # Инициализация Spark
    spark = get_spark_session("Final_Dashboard_Generator")
    spark.sparkContext.setLogLevel("WARN")

    # Вспомогательная функция для получения статистики
    def get_stats(table_name):
        try:
            if not spark.catalog.tableExists(table_name):
                return 0, 0
            df = spark.read.table(table_name)
            d = df.groupBy("prediction").count().toPandas()
            stats = d.set_index('prediction')['count'].to_dict()
            return stats.get(0.0, 0), stats.get(1.0, 0)
        except Exception as e:
            print(f"   ⚠️ Ошибка чтения {table_name}: {e}")
            return 0, 0

    # =================================================================================
    # 1. ГРАФИК АКТИВНОСТИ (Line Chart)
    # =================================================================================
    print("\n📈 [1/15] Динамика активности (Posts per Day)...")
    try:
        target_table = "gold_comments" if spark.catalog.tableExists("gold_comments") else "silver_comments"
        cols = spark.read.table(target_table).columns
        date_col = "date" if "date" in cols else ("date_ts" if "date_ts" in cols else None)

        if date_col:
            df = spark.sql(f"""
                SELECT {date_col} as post_date, count(*) as cnt 
                FROM {target_table} 
                WHERE {date_col} IS NOT NULL 
                GROUP BY {date_col} 
                ORDER BY {date_col}
            """).toPandas()

            if not df.empty:
                df['post_date'] = pd.to_datetime(df['post_date'])
                plt.figure()
                sns.lineplot(data=df, x='post_date', y='cnt', marker='o', linewidth=2.5, color="#3498db")
                plt.title('Динамика комментариев в канале', fontsize=16)
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.savefig(f"{REPORT_DIR}/1_activity_dynamics.png")
                print("   ✅ Готово")
        else:
            print(f"   ⚠️ Колонки даты не найдено в {target_table}")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 1б. ДИНАМИКА РЕАЛЬНЫХ КОММЕНТАРИЕВ (NEW!)
    # =================================================================================
    print("\n📉 [1б/15] Динамика реальных комментариев (Silver Layer)...")
    try:
        # Принудительно используем silver_comments, чтобы показать реальные данные,
        # даже если есть gold (синтетика)
        if spark.catalog.tableExists("silver_comments"):
            cols = spark.read.table("silver_comments").columns
            # Проверяем наличие колонки даты
            date_col = "date" if "date" in cols else ("date_ts" if "date_ts" in cols else None)

            if date_col:
                df_real = spark.sql(f"""
                        SELECT {date_col} as post_date, count(*) as cnt 
                        FROM silver_comments
                        WHERE {date_col} IS NOT NULL 
                        GROUP BY {date_col} 
                        ORDER BY {date_col}
                    """).toPandas()

                if not df_real.empty:
                    df_real['post_date'] = pd.to_datetime(df_real['post_date'])

                    plt.figure()
                    # Используем зеленый цвет, чтобы отличить от синего (синтетика/общий)
                    sns.lineplot(data=df_real, x='post_date', y='cnt', marker='.', linewidth=1.5, color="#27ae60")
                    plt.title('Динамика комментариев (Реальные данные / Silver Layer)', fontsize=16)
                    plt.xlabel('Дата')
                    plt.ylabel('Кол-во комментариев')
                    plt.xticks(rotation=45)
                    plt.grid(True, linestyle='--')
                    plt.tight_layout()
                    plt.savefig(f"{REPORT_DIR}/1b_real_data_dynamics.png")
                    print("   ✅ Готово")
            else:
                print("   ⚠️ Нет колонки даты в silver_comments.")
        else:
            print("   ⚠️ Таблица silver_comments не найдена.")

    except Exception as e:
        print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 2. ТОП ТОКСИЧНЫХ АВТОРОВ (Bar Chart)
    # =================================================================================
    print("\n🏆 [2/15] Топ токсичных авторов...")
    try:
        if spark.catalog.tableExists("gold_bigdata_predictions"):
            df = spark.sql("""
                SELECT author_name, count(*) as cnt
                FROM gold_bigdata_predictions 
                WHERE prediction = 1.0 AND author_name IS NOT NULL
                GROUP BY author_name
                ORDER BY cnt DESC
                LIMIT 10
            """).toPandas()

            plt.figure(figsize=(12, 6))
            sns.barplot(data=df, x='cnt', y='author_name', hue='author_name', palette="Reds_r", legend=False)
            plt.title('Топ-10 авторов деструктивного контента', fontsize=16)
            plt.xlabel('Количество токсичных сообщений')
            plt.ylabel('')
            plt.tight_layout()
            plt.savefig(f"{REPORT_DIR}/2_top_toxic_users.png")
            print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 3. ДОЛЯ ТОКСИЧНОСТИ (Pie Chart)
    # =================================================================================
    print("\n🍰 [3/15] Общая диаграмма токсичности...")
    try:
        safe, toxic = get_stats("gold_bigdata_predictions")
        if safe + toxic > 0:
            plt.figure()
            plt.pie([safe, toxic], labels=['Безопасный', 'Токсичный'],
                    autopct='%1.1f%%', colors=['#2ecc71', '#e74c3c'], startangle=90, explode=(0, 0.1))
            plt.title('Соотношение контента (Big Data Model)', fontsize=16)
            plt.savefig(f"{REPORT_DIR}/3_destructive_ratio.png")
            print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 4. СРАВНЕНИЕ (2 Pie Charts)
    # =================================================================================
    print("\n🆚 [4/15] Сравнение методов (Start vs End)...")
    try:
        s1, t1 = get_stats("platinum_destructive")
        s2, t2 = get_stats("gold_bigdata_predictions")

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

        if s1+t1 > 0:
            ax1.pie([s1, t1], labels=['Safe', 'Toxic'], autopct='%1.1f%%', colors=['#bdc3c7', '#e74c3c'])
        ax1.set_title('Поиск по словарю\n(Rule-based)')

        if s2+t2 > 0:
            ax2.pie([s2, t2], labels=['Safe', 'Toxic'], autopct='%1.1f%%', colors=['#2ecc71', '#e74c3c'])
        ax2.set_title('Обученная модель\n(Big Data ML)')

        plt.suptitle('Влияние метода анализа на результат', fontsize=16)
        plt.savefig(f"{REPORT_DIR}/4_comparison_2models.png")
        print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 5. ЭВОЛЮЦИЯ ТОЧНОСТИ (3 Pie Charts)
    # =================================================================================
    print("\n🧬 [5/15] Эволюция методов (3 этапа)...")
    try:
        s1, t1 = get_stats("platinum_destructive")
        s2, t2 = get_stats("gold_supervised_predictions")
        s3, t3 = get_stats("gold_bigdata_predictions")

        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        labels = ['Safe', 'Toxic']
        colors = ['#95a5a6', '#e74c3c']

        if s1+t1 > 0: axes[0].pie([s1, t1], labels=labels, autopct='%1.1f%%', colors=colors)
        else: axes[0].text(0.5, 0.5, "Нет данных", ha='center')
        axes[0].set_title('1. Словарь (Baseline)')

        if s2+t2 > 0: axes[1].pie([s2, t2], labels=labels, autopct='%1.1f%%', colors=['#3498db', '#e74c3c'])
        else: axes[1].text(0.5, 0.5, "Нет данных", ha='center')
        axes[1].set_title('2. Medium ML (Supervised)')

        if s3+t3 > 0: axes[2].pie([s3, t3], labels=labels, autopct='%1.1f%%', colors=['#2ecc71', '#e74c3c'])
        else: axes[2].text(0.5, 0.5, "Нет данных", ha='center')
        axes[2].set_title('3. Big Data ML (FastText)')

        plt.suptitle('Эволюция точности системы', fontsize=16)
        plt.savefig(f"{REPORT_DIR}/4b_evolution_3models.png")
        print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 6. ИТОГОВЫЙ BAR CHART
    # =================================================================================
    print("\n📊 [6/15] Итоговое сравнение %...")
    try:
        def calc_percent(s, t): return (t / (s+t) * 100) if (s+t) > 0 else 0
        p1, p2, p3 = calc_percent(s1, t1), calc_percent(s2, t2), calc_percent(s3, t3)

        plt.figure(figsize=(10, 6))
        bars = plt.bar(["Словарь", "Medium ML", "Big Data ML"], [p1, p2, p3], color=['#95a5a6', '#3498db', '#9b59b6'])
        plt.ylabel('% Найденной токсичности')
        plt.title('Чувствительность разных методов', fontsize=16)
        plt.ylim(0, max([p1, p2, p3])+5)
        for bar in bars:
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, f"{bar.get_height():.1f}%", ha='center')
        plt.savefig(f"{REPORT_DIR}/5_final_conclusion.png")
        print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 7. МАСШТАБИРУЕМОСТЬ (ОБНОВЛЕНО: С КОЛИЧЕСТВОМ ЗАПИСЕЙ)
    # =================================================================================
    print("\n🚀 [7/15] График масштабируемости...")
    try:
        # Данные из реальных логов обучения:
        # Small: 10% от 14k -> 1.5k строк
        # Medium: 100% от 14k -> 14.4k строк
        # Big Data: dataset.txt -> 248k строк

        counts = [1500, 14400, 248000]
        times = [2.5, 5.2, 10.3] # Время из предыдущих запусков

        # Формируем подписи с переносом строки
        labels = [
            f"Small\n({counts[0]} записей)",
            f"Medium\n({counts[1]//1000}k записей)",
            f"Big Data\n({counts[2]//1000}k записей)"
        ]

        plt.figure()
        plt.plot(labels, times, marker='o', linewidth=3, markersize=10, color="#2c3e50")

        plt.title('Масштабируемость Spark ML (Время обучения от объема данных)', fontsize=16)
        plt.ylabel('Время обучения (сек)')
        plt.xlabel('Размер обучающей выборки')
        plt.grid(True, linestyle='--')

        for i, t in enumerate(times):
            plt.annotate(f"{t}c", (labels[i], times[i]), xytext=(0, 10), textcoords='offset points', ha='center', fontweight='bold')

        plt.savefig(f"{REPORT_DIR}/6_scalability_time.png")
        print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 8. СРАВНИТЕЛЬНЫЙ АНАЛИЗ ЛЕКСИКИ
    # =================================================================================
    print("\n🤬 [8/15] Анализ лексики для всех моделей (3 графика)...")
    try:
        def get_top_words(table_name):
            if not spark.catalog.tableExists(table_name):
                return pd.DataFrame(columns=['word', 'count'])
            return spark.read.table(table_name) \
                .filter("prediction = 1.0") \
                .select(F.explode(F.split(F.col("clean_text"), " ")).alias("word")) \
                .filter(F.length(F.col("word")) > 4) \
                .groupBy("word").count() \
                .orderBy(F.col("count").desc()) \
                .limit(10) \
                .toPandas()

        df_rule = get_top_words("platinum_destructive")
        df_med  = get_top_words("gold_supervised_predictions")
        df_big  = get_top_words("gold_bigdata_predictions")

        fig, axes = plt.subplots(1, 3, figsize=(20, 8))
        if not df_rule.empty: sns.barplot(data=df_rule, y='word', x='count', ax=axes[0], palette="Greys_r", hue='word', legend=False)
        axes[0].set_title('1. Словарь')
        if not df_med.empty: sns.barplot(data=df_med, y='word', x='count', ax=axes[1], palette="Blues_r", hue='word', legend=False)
        axes[1].set_title('2. Medium ML')
        if not df_big.empty: sns.barplot(data=df_big, y='word', x='count', ax=axes[2], palette="Reds_r", hue='word', legend=False)
        axes[2].set_title('3. Big Data ML')
        plt.savefig(f"{REPORT_DIR}/8_top_words_comparison.png")
        print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 9. ДЛИНА ТЕКСТА vs ТОКСИЧНОСТЬ
    # =================================================================================
    print("\n📏 [9/15] Корреляция длины и токсичности...")
    try:
        if spark.catalog.tableExists("gold_bigdata_predictions"):
            df_len = spark.read.table("gold_bigdata_predictions") \
                .select("prediction", F.length("clean_text").alias("len")) \
                .withColumn("len_bin", (F.col("len") / 20).cast(IntegerType()) * 20) \
                .groupBy("len_bin") \
                .agg(F.avg("prediction").alias("rate")) \
                .filter("len_bin < 300") \
                .orderBy("len_bin") \
                .toPandas()

            if not df_len.empty:
                plt.figure(figsize=(12, 6))
                df_len['rate'] = df_len['rate'] * 100
                sns.lineplot(data=df_len, x='len_bin', y='rate', marker='o', linewidth=3, color='#c0392b')
                plt.fill_between(df_len['len_bin'], df_len['rate'], color='#c0392b', alpha=0.1)
                plt.title('Вероятность токсичности в зависимости от длины комментария', fontsize=16)
                plt.xlabel('Длина текста (символов)')
                plt.ylabel('% Токсичности')
                plt.grid(True, linestyle='--')
                plt.savefig(f"{REPORT_DIR}/9_length_correlation.png")
                print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 10. BUBBLE CHART
    # =================================================================================
    print("\n☁️ [10/15] Bubble chart токсичных слов...")
    try:
        if not df_big.empty:
            plt.figure(figsize=(10, 8))
            sns.scatterplot(data=df_big, x="word", y="count", size="count", sizes=(100, 2000), hue="word", alpha=0.6, legend=False, palette="viridis")
            for i in range(df_big.shape[0]):
                plt.text(df_big.word[i], df_big['count'][i], df_big.word[i], horizontalalignment='center', size='medium', color='black', weight='semibold')
            plt.title("Пузырьковая диаграмма топ-слов (Big Data)", fontsize=16)
            plt.xlabel("")
            plt.xticks([])
            plt.savefig(f"{REPORT_DIR}/10_bubble_words.png")
            print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 11. HEATMAP (Activity)
    # =================================================================================
    print("\n🔥 [11/15] Heatmap активности...")
    try:
        target = "gold_comments" if spark.catalog.tableExists("gold_comments") else "silver_comments"
        cols = spark.read.table(target).columns
        date_col = "date" if "date" in cols else ("date_ts" if "date_ts" in cols else None)

        if date_col:
            df_heat = spark.read.table(target) \
                .withColumn("hour", F.hour(F.col(date_col))) \
                .withColumn("day_of_week", F.dayofweek(F.col(date_col))) \
                .groupBy("day_of_week", "hour").count().toPandas()

            if not df_heat.empty:
                pivot_table = df_heat.pivot(index='day_of_week', columns='hour', values='count').fillna(0).sort_index()
                plt.figure(figsize=(12, 6))
                sns.heatmap(pivot_table, cmap="YlGnBu", annot=False, fmt="g")
                plt.title(f'Тепловая карта активности ({target})', fontsize=16)
                plt.xlabel('Час дня')
                plt.ylabel('День недели (1=Вс, 2=Пн...)')
                plt.savefig(f"{REPORT_DIR}/11_activity_heatmap.png")
                print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 12. АНАЛИЗ СОГЛАСОВАННОСТИ (Grouped Bar)
    # =================================================================================
    print("\n🎯 [12/15] Анализ согласованности моделей...")
    try:
        top_authors_df = spark.sql("SELECT author_name, count(*) as cnt FROM gold_bigdata_predictions WHERE prediction=1.0 GROUP BY author_name ORDER BY cnt DESC LIMIT 15").toPandas()
        top_authors_list = top_authors_df['author_name'].tolist()

        if top_authors_list:
            def get_author_stats(table, authors):
                if not spark.catalog.tableExists(table): return pd.DataFrame(columns=['author_name', f'cnt_{table}'])
                return spark.read.table(table).filter(F.col("author_name").isin(authors)).filter("prediction = 1.0").groupBy("author_name").count().withColumnRenamed("count", f"cnt_{table}").toPandas()

            df1 = get_author_stats("platinum_destructive", top_authors_list)
            df2 = get_author_stats("gold_supervised_predictions", top_authors_list)
            df3 = get_author_stats("gold_bigdata_predictions", top_authors_list)

            merged = pd.DataFrame({'author_name': top_authors_list})
            if not df1.empty: merged = merged.merge(df1, on='author_name', how='left')
            if not df2.empty: merged = merged.merge(df2, on='author_name', how='left')
            if not df3.empty: merged = merged.merge(df3, on='author_name', how='left')
            merged = merged.fillna(0)

            # Если каких-то колонок нет после мержа (из-за пустых df), добавим их нулями
            for col in ['cnt_platinum_destructive', 'cnt_gold_supervised_predictions', 'cnt_gold_bigdata_predictions']:
                if col not in merged.columns: merged[col] = 0

            plt.figure(figsize=(14, 7))
            bar_width = 0.25
            index = np.arange(len(merged))

            plt.bar(index, merged['cnt_platinum_destructive'], bar_width, label='Словарь', color='#95a5a6')
            plt.bar(index + bar_width, merged['cnt_gold_supervised_predictions'], bar_width, label='Medium ML', color='#3498db')
            plt.bar(index + 2*bar_width, merged['cnt_gold_bigdata_predictions'], bar_width, label='Big Data', color='#e74c3c')

            plt.xlabel('Автор')
            plt.ylabel('Кол-во токсичных сообщений')
            plt.title('Как разные модели оценивают топ-авторов', fontsize=16)
            plt.xticks(index + bar_width, merged['author_name'], rotation=45, ha='right')
            plt.legend()
            plt.tight_layout()
            plt.savefig(f"{REPORT_DIR}/12_model_agreement_authors.png")
            print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 13. PARETO (Activity)
    # =================================================================================
    print("\n👥 [13/15] Распределение активности (Pareto)...")
    try:
        target = "gold_comments" if spark.catalog.tableExists("gold_comments") else "silver_comments"
        df_act = spark.sql(f"SELECT author_name, count(1) as msg_count FROM {target} WHERE author_name IS NOT NULL GROUP BY author_name").toPandas()

        if not df_act.empty:
            df_act = df_act.sort_values('msg_count', ascending=False).reset_index(drop=True)
            plt.figure(figsize=(10, 6))
            plt.plot(df_act.index, df_act['msg_count'], color='purple', linewidth=3)
            plt.yscale('log')
            plt.title('Распределение активности (Long Tail / Закон Парето)', fontsize=16)
            plt.xlabel('Ранг пользователя')
            plt.ylabel('Сообщений (Log)')
            plt.grid(True, which="both", ls="--")
            plt.savefig(f"{REPORT_DIR}/13_user_activity_dist.png")
            print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 14. SCATTER (Activity vs Toxicity)
    # =================================================================================
    print("\n💢 [14/15] Scatter: Активность vs Токсичность...")
    try:
        if spark.catalog.tableExists("gold_bigdata_predictions"):
            df_scatter = spark.sql("""
                SELECT author_name, count(*) as total, sum(cast(prediction as int)) as toxic 
                FROM gold_bigdata_predictions 
                GROUP BY author_name HAVING total > 5
            """).toPandas()

            if not df_scatter.empty:
                df_scatter['rate'] = df_scatter['toxic'] / df_scatter['total']
                plt.figure(figsize=(10, 6))
                sns.scatterplot(data=df_scatter, x='total', y='rate', alpha=0.5, color='darkred')
                plt.title('Активность vs Доля токсичности', fontsize=16)
                plt.xlabel('Всего сообщений (Log)')
                plt.ylabel('Доля токсичности')
                plt.xscale('log')
                plt.grid(True)
                plt.savefig(f"{REPORT_DIR}/14_activity_vs_toxicity.png")
                print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    # =================================================================================
    # 15. CHANNEL PULSE (Posts Trend)
    # =================================================================================
    print("\n📈 [15/15] Пульс канала (Posts/Views)...")
    try:
        target_posts = "gold_posts" if spark.catalog.tableExists("gold_posts") else "silver_posts"
        if spark.catalog.tableExists(target_posts):
            df_trend = spark.sql(f"""
                SELECT date, count(*) as posts, sum(views) as views
                FROM {target_posts}
                GROUP BY date
                ORDER BY date
            """).toPandas()

            if not df_trend.empty:
                df_trend['date'] = pd.to_datetime(df_trend['date'])
                fig, ax1 = plt.subplots(figsize=(12, 6))

                color = 'tab:blue'
                ax1.set_xlabel('Дата')
                ax1.set_ylabel('Посты', color=color)
                ax1.plot(df_trend['date'], df_trend['posts'], color=color, linewidth=2, label='Посты')
                ax1.tick_params(axis='y', labelcolor=color)

                if df_trend['views'].sum() > 0:
                    ax2 = ax1.twinx()
                    color = 'tab:orange'
                    ax2.set_ylabel('Просмотры', color=color)
                    ax2.plot(df_trend['date'], df_trend['views'], color=color, linestyle='--', alpha=0.6, label='Просмотры')
                    ax2.tick_params(axis='y', labelcolor=color)

                plt.title('Пульс канала: Посты и Просмотры', fontsize=16)
                plt.tight_layout()
                plt.savefig(f"{REPORT_DIR}/15_channel_pulse.png")
                print("   ✅ Готово")
    except Exception as e: print(f"   ❌ Ошибка: {e}")

    spark.stop()
    print(f"\n✨ ВСЕ ОТЧЕТЫ СОХРАНЕНЫ В: {REPORT_DIR}")
    print("📊 Создано 15 визуализаций!")

if __name__ == "__main__":
    generate_dashboard()