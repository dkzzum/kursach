import os
import sys
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- НАСТРОЙКИ СТИЛЯ ---
plt.rcParams.update({'figure.figsize': (14, 8), 'figure.dpi': 100})
sns.set_theme(style="whitegrid")

# Папка для отчетов
REPORT_DIR = "/data/reports"
os.makedirs(REPORT_DIR, exist_ok=True)


def generate_dashboard():
    print("\n" + "=" * 50)
    print("🚀 ГЕНЕРАЦИЯ DASHBOARD: АНАЛИЗ ТОКСИЧНОСТИ (HIVE EDITION)")
    print("=" * 50)

    # Инициализация Spark с поддержкой Hive
    spark = SparkSession.builder \
        .appName("Dashboard_Generator") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Имя таблицы, которую мы создали на прошлом шаге
    TABLE_NAME = "gold_toxic_predictions"

    print(f"📊 Подключение к таблице Hive: {TABLE_NAME}...")

    try:
        if not spark.catalog.tableExists(TABLE_NAME):
            print(f"❌ Таблица {TABLE_NAME} не найдена. Сначала запустите ML пайплайн.")
            return

        # Читаем данные как таблицу (SQL-style)
        df = spark.table(TABLE_NAME)

        # Фильтруем только токсичные (is_toxic_pred = 1.0)
        toxic_df = df.filter("is_toxic_pred = 1.0")
        count = toxic_df.count()
        print(f"🤬 Найдено токсичных комментариев для анализа: {count}")

        # --- ЛОГИКА WORD COUNT ---
        print("🔠 Подсчет частотности слов...")

        STOP_WORDS_RU = [
            "только", "просто", "вообще", "почему", "ничего", "когда", "сейчас",
            "какой", "такой", "можно", "надо", "есть", "будет", "если", "очень",
            "даже", "теперь", "тоже", "тебе", "меня", "этого", "этом", "всем",
            "чтобы", "тебя", "таких", "было", "может", "потом", "этих", "всех",
            "которые", "нужно", "такие", "пусть", "сразу", "потому", "лучше", "это", "как",
            'больше', 'себе', 'этот', 'себя', 'тогда', 'такое', 'будут', 'быть', 'люди',
            'зачем', 'людей', 'хоть', 'который', 'после', 'через', 'один', 'конечно', 'него', 'всегда', 'давно',
            'чего', 'пока', 'чтоб', 'того', 'этим', 'была', 'кстати', 'хотя', 'много', 'делать',
            'россии', 'туда', 'какая', 'могут', 'реально', 'куда', 'детей', 'опять', 'понял', 'кого',
            'свою', 'свои', 'этой', 'значит', 'никто', 'более', 'своей', 'сделать', 'были', 'скоро',
            'человек', 'свой', 'какие', 'прям', 'всего', 'своими', 'твой', 'жизнь'
        ]

        # Разделение на слова -> Очистка -> Группировка
        top_words_df = toxic_df \
            .select(F.explode(F.split(F.col("original_content"), " ")).alias("word")) \
            .withColumn("word", F.lower(F.regexp_replace(F.col("word"), r"[^а-яА-Яa-zA-Z]", ""))) \
            .filter(F.length(F.col("word")) > 3) \
            .filter(~F.col("word").isin(STOP_WORDS_RU)) \
            .groupBy("word").count() \
            .orderBy(F.col("count").desc()) \
            .limit(15) \
            .toPandas()

        # --- ОТРИСОВКА ---
        if not top_words_df.empty:
            print("🎨 Рисуем график...")
            plt.figure(figsize=(12, 8))

            sns.barplot(
                data=top_words_df,
                y='word',
                x='count',
                palette="Reds_r",
                hue='word',
                legend=False
            )

            plt.title('Топ-15 слов в токсичных комментариях (LogReg Analysis)', fontsize=16)
            plt.xlabel("Количество упоминаний")
            plt.ylabel("Слово")

            # Сохранение
            save_path = os.path.join(REPORT_DIR, "toxic_words_hive.png")
            plt.tight_layout()
            plt.savefig(save_path)
            print(f"✅ График сохранен: {save_path}")
        else:
            print("⚠️ Данных недостаточно для графика.")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()


if __name__ == "__main__":
    generate_dashboard()