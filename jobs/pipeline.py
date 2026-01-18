import time
import subprocess
import os
import sys
from datetime import datetime


class DataPipeline:
    def __init__(self):
        # --- НАСТРОЙКИ ---
        self.DEMO_MODE = True  # True = цикл 5 минут, False = цикл 24 часа

        # Интервалы
        self.SCRAPER_DURATION = 60 if self.DEMO_MODE else 3600  # Время работы парсера (сек)
        self.CYCLE_SLEEP = 300 if self.DEMO_MODE else 86400  # Пауза между циклами (сек)

        # --- ПУТИ К СКРИПТАМ ---
        self.BASE_DIR = "/app/src/app"

        # 1. Парсер
        self.SCRAPER_SCRIPT = os.path.join(self.BASE_DIR, "scraper/telegram_scraper.py")

        # 2. ETL (Бронза -> Сильвер)
        self.ETL_SCRIPT = os.path.join(self.BASE_DIR, "etl/bronze_to_silver.py")

        # 3. ML (Логистическая регрессия)
        self.ML_SCRIPT = os.path.join(self.BASE_DIR, "ml/train_log_regression_dataset.py")

        # 4. [NEW] Аналитика (Дашборды и проверка для Superset)
        self.ANALYTICS_SCRIPT = os.path.join(self.BASE_DIR, "analytics/dashboard_lr.py")

        # 5. Очистка
        self.CLEANUP_SCRIPT = os.path.join(self.BASE_DIR, "maintenance/clean_old_data.py")

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🤖 {msg}", flush=True)

    def run_scraper(self):
        self.log(f"STAGE 1: Сбор данных ({self.SCRAPER_DURATION} сек)...")
        if not os.path.exists(self.SCRAPER_SCRIPT):
            self.log(f"❌ Ошибка: Скрипт парсера не найден: {self.SCRAPER_SCRIPT}")
            return

        proc = subprocess.Popen(["python", self.SCRAPER_SCRIPT])
        try:
            time.sleep(self.SCRAPER_DURATION)
        except KeyboardInterrupt:
            pass
        finally:
            self.log("Остановка парсера...")
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except:
                proc.kill()
            self.log("✅ Сбор данных завершен.")

    def run_task(self, script_path, task_name):
        self.log(f"STAGE {task_name}: Запуск...")

        if not os.path.exists(script_path):
            self.log(f"❌ Ошибка: Скрипт не найден: {script_path}")
            return False

        # capture_output=False чтобы видеть логи в реальном времени
        result = subprocess.run(["python", script_path], capture_output=False)

        if result.returncode == 0:
            self.log(f"✅ {task_name} успешно выполнен.")
            return True
        else:
            self.log(f"❌ {task_name} упал с кодом {result.returncode}.")
            return False

    def start(self):
        mode_str = "DEMO (5 мин)" if self.DEMO_MODE else "PROD (24 часа)"
        self.log(f"🚀 Запуск конвейера [{mode_str}]")

        while True:
            start_ts = time.time()
            self.log("=" * 50)
            self.log("▶️ НАЧАЛО ЦИКЛА")
            self.log("=" * 50)

            # 1. СБОР
            self.run_scraper()

            # 2. ETL
            if self.run_task(self.ETL_SCRIPT, "2 (ETL Bronze->Silver)"):

                # 3. ML
                if self.run_task(self.ML_SCRIPT, "3 (ML LogReg)"):
                    # 4. [NEW] АНАЛИТИКА / SUPERSET READY CHECK
                    self.run_task(self.ANALYTICS_SCRIPT, "4 (Analytics & Superset Check)")

                # 5. ОЧИСТКА (в конце цикла)
                self.run_task(self.CLEANUP_SCRIPT, "5 (Cleanup)")

            else:
                self.log("⚠️ Пропуск ML и Аналитики из-за ошибки в ETL.")

            # СОН
            elapsed = time.time() - start_ts
            sleep_time = max(0, self.CYCLE_SLEEP - elapsed)

            self.log(f"💤 Цикл завершен за {int(elapsed)} сек. Сон {int(sleep_time)} сек...")
            self.log(f"📊 Superset доступен по адресу: http://localhost:8088")
            time.sleep(sleep_time)


if __name__ == "__main__":
    time.sleep(5)
    pipeline = DataPipeline()
    pipeline.start()
