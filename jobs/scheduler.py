import time
import schedule
import subprocess
import datetime
import sys
import os

# --- НАСТРОЙКИ ---
# TRUE = Демонстрация (запуск каждые 5 минут, чтобы успевал отработать Big Data)
# FALSE = Продакшн (запуск раз в сутки в 00:00)
DEMO_MODE = True

# Имя контейнера из docker-compose
DOCKER_CONTAINER = "spark_master"


def run_pipeline():
    print(f"\n" + "=" * 50)
    print(f"⏰ [START] ЗАПУСК ЦИКЛА ОБРАБОТКИ: {datetime.datetime.now().strftime('%H:%M:%S')}")
    print("=" * 50)

    # 1. СБОР ДАННЫХ (Запускается локально на хосте, так как там лежит сессия Telegram)
    print("\n🕷 Шаг 1: Сбор новых данных (Telegram Scraper)...")
    try:
        # Запускаем скрапер локальным питоном
        scraper_path = os.path.join("app", "scraper", "telegram_scraper.py")
        if os.path.exists(scraper_path):
            subprocess.run([sys.executable, scraper_path], check=True)
            print("✅ Скрапинг завершен.")
        else:
            print(f"⚠️ Скрапер не найден по пути {scraper_path}, пропускаем.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка парсера: {e}")

    # 2. ОБРАБОТКА В DOCKER (Spark Pipeline)
    print(f"\n⚙️ Шаг 2: Обработка данных внутри контейнера '{DOCKER_CONTAINER}'...")

    # Список задач: [Описание, Команда внутри Docker]
    docker_tasks = [
        # ETL: Загрузка JSON -> Hive Silver
        ("ETL: Bronze -> Silver", ["python3", "/app/etl/bronze_to_silver.py"]),

        # Generator: Создание синтетики для нагрузочного теста (Silver -> Gold)
        ("Data Gen: Синтетическая генерация (Big Data)", ["python3", "/app/etl/data_generator.py"]),

        # ML: Обучение и предсказание (используем самую мощную модель)
        ("ML: Обучение модели и классификация", ["python3", "/app/ml/train_big_dataset.py"]),

        # Analytics: Генерация всех графиков и отчетов
        ("Analytics: Генерация финального дашборда", ["python3", "/app/analytics/final_dashboard.py"])
    ]

    for description, command in docker_tasks:
        print(f"   -> Запуск {description}...")
        try:
            # Формируем команду: docker exec -i spark_master python3 /app/path/to/script.py
            full_cmd = ["docker", "exec", "-i", DOCKER_CONTAINER] + command

            start_t = time.time()
            subprocess.run(full_cmd, check=True)
            duration = time.time() - start_t

            print(f"      ✅ Успешно ({duration:.1f} сек)")

        except subprocess.CalledProcessError:
            print(f"      ❌ Ошибка при выполнении этапа. Переходим к следующему...")

    print(f"\n✅ [DONE] Цикл полностью завершен в {datetime.datetime.now().strftime('%H:%M:%S')}")
    print("💤 Ожидание следующего запуска...")


# --- ЗАПУСК ---

print("🚀 ОРКЕСТРАТОР ЗАПУЩЕН")
print(f"Режим: {'DEMO (интервал 5 мин)' if DEMO_MODE else 'PROD (00:00 ежедневно)'}")

# Запланировать задачи
if DEMO_MODE:
    # Ставим 5 минут, так как обучение BigData модели + генерация 2.5 млн строк
    # может занимать 1-2 минуты.
    schedule.every(5).minutes.do(run_pipeline)
else:
    schedule.every().day.at("00:00").do(run_pipeline)

# Запустить первый раз сразу, чтобы проверить работоспособность
run_pipeline()

# Бесконечный цикл ожидания
while True:
    schedule.run_pending()
    time.sleep(1)
