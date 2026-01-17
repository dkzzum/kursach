import os
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import functions as F

# Настройки стиля
plt.rcParams.update({'figure.figsize': (14, 8), 'figure.dpi': 100})
sns.set_theme(style="whitegrid")

# Путь к корню проекта
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.core.config import get_spark_session

# Папка для отчета
REPORT_DIR = "/data/reports"
os.makedirs(REPORT_DIR, exist_ok=True)


def generate_dashboard():
    print("\n" + "=" * 50)
    print("🚀 ГЕНЕРАЦИЯ DASHBOARD: АНАЛИЗ ТОКСИЧНОЙ ЛЕКСИКИ")
    print("=" * 50)

    spark = get_spark_session("Dashboard_LogReg")
    spark.sparkContext.setLogLevel("ERROR")

    # Путь к данным Gold (результаты предикта)
    gold_path = "/data/gold/predictions"

    # Расширенный список стоп-слов для русского языка
    STOP_WORDS_RU = [
        "только", "просто", "вообще", "почему", "ничего", "когда", "сейчас",
        "какой", "такой", "можно", "надо", "есть", "будет", "если", "очень",
        "даже", "теперь", "тоже", "тебе", "меня", "этого", "этом", "всем",
        "чтобы", "тебя", "таких", "было", "может", "потом", "этих", "всех",
        "которые", "нужно", "такие", "пусть", "сразу", "потому", "лучше",
        "если", "хотя", "через", "около", "будто", "кажется", "почти"
    ]

    try:
        if not os.path.exists(gold_path):
            print(f"❌ Путь {gold_path} не найден. Сначала запустите ML скрипт.")
            return

        print(f"📖 Чтение данных из Parquet: {gold_path}")
        # Читаем данные напрямую из папки
        df = spark.read.parquet(gold_path)

        # Обработка слов
        print("⚙️ Выполняю Word Count для токсичных комментариев...")
        top_words_df = df.filter(F.col("is_toxic_pred") == 1.0) \
            .select(F.explode(F.split(F.col("original_content"), " ")).alias("word")) \
            .withColumn("word", F.lower(F.regexp_replace(F.col("word"), r"[^а-яА-Яa-zA-Z]", ""))) \
            .filter(F.length(F.col("word")) > 3) \
            .filter(~F.col("word").isin(STOP_WORDS_RU)) \
            .groupBy("word").count() \
            .orderBy(F.col("count").desc()) \
            .limit(15) \
            .toPandas()

        if top_words_df.empty:
            print("⚠️ Нет данных для отображения (возможно, токсичных комментариев не найдено).")
            return

        # Построение графика
        print("📊 Отрисовка графика...")
        plt.figure(figsize=(12, 8))

        # Используем современный синтаксис seaborn
        plot = sns.barplot(
            data=top_words_df,
            x='count',
            y='word',
            palette="flare",
            hue='word',
            legend=False
        )

        plt.title('Топ-15 слов в токсичных комментариях (Logistic Regression)', fontsize=16)
        plt.xlabel('Количество упоминаний', fontsize=12)
        plt.ylabel('Слова', fontsize=12)
        plt.grid(axis='x', linestyle='--', alpha=0.7)

        # Сохранение
        save_path = os.path.join(REPORT_DIR, "toxic_words_chart.png")
        plt.tight_layout()
        plt.savefig(save_path)
        print(f"✅ График успешно сохранен: {save_path}")

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    generate_dashboard()