import time
import schedule
import subprocess
import datetime
import sys
import os

# --- НАСТРОЙКИ ---
# TRUE = Демонстрация (запуск раз в 2 минуты)
# FALSE = Продакшн (запуск раз в сутки в 00:00)
DEMO_MODE = True


def run_pipeline():
    print(f"\n⏰ [START] Запуск цикла обработки: {datetime.datetime.now()}")

    # 1. СБОР ДАННЫХ (Запускается локально на Python)
    print("🕷 Шаг 1: Запуск парсера Telegram...")
    try:
        # sys.executable гарантирует использование того же python
        subprocess.run([sys.executable, "app/scraper/telegram_scraper.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка парсера: {e}")
        # Если критично, можно добавить return, но для курсовой лучше продолжить

    # 2. ОБРАБОТКА В DOCKER (Spark)
    print("⚙️ Шаг 2: Обработка данных внутри Spark-контейнера...")

    docker_tasks = [
        # Bronze -> Silver (Spark сам найдет новые файлы и добавит их)
        ["python", "app/etl/bronze_to_silver.py"],

        # Generator (раздуваем данные)
        ["python", "app/etl/data_generator.py"],

        # ML Классификация (обучаем модель на полных данных)
        # Используем Big Data скрипт для максимального качества
        ["python", "app/ml/train_big_dataset.py"],

        # Обновление отчетов
        ["python", "app/analytics/mega_report.py"],
        ["python", "app/analytics/scalability_report.py"]
    ]

    for task in docker_tasks:
        script = task[1]
        print(f"   -> Запуск {script}...")
        try:
            # Команда: docker exec -i spark_processor python script_name.py
            cmd = ["docker", "exec", "-i", "spark_processor"] + task
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError:
            print(f"❌ Ошибка при выполнении {script}. Идем дальше...")

    print(f"✅ [DONE] Цикл завершен в {datetime.datetime.now()}\n")
    print("💤 Ожидание следующего запуска...")


# --- ЗАПУСК ---

print("🚀 ОРКЕСТРАТОР ЗАПУЩЕН")
print(f"Режим: {'DEMO (каждые 2 мин)' if DEMO_MODE else 'PROD (00:00 ежедневно)'}")

# Запланировать задачи
if DEMO_MODE:
    schedule.every(2).minutes.do(run_pipeline)
else:
    schedule.every().day.at("00:00").do(run_pipeline)

# Запустить первый раз сразу (чтобы не ждать таймера для проверки)
run_pipeline()

while True:
    schedule.run_pending()
    time.sleep(1)
