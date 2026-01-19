# Big Data Pipeline: Анализ Токсичности в Telegram

Комплексная система для сбора, процессинга, генерации и ML-анализа сообщений из Telegram. Проект демонстрирует полный цикл работы с большими данными: от ETL-процессов на Apache Spark до визуализации в Apache Superset.

## Стек технологий

* **Инфраструктура:** Docker, Docker Compose
* **ETL & Processing:** Apache Spark (PySpark)
* **Storage:** Apache Hive (Metastore + Warehouse)
* **ML:** Spark MLlib (Logistic Regression, Random Forest)
* **Analytics:** Apache Superset
* **Language:** Python 3.9

## Структура репозитория

```text
├── config/             # Конфигурации и зависимости
├── data/               # Локальное хранилище данных (Raw JSON, CSV)
├── docker/             # Docker-файлы и docker-compose.yml
├── scripts/            # Bash-скрипты для быстрого запуска
├── src/
│   └── app/
│       ├── etl/        # Скрипты обработки данных (Bronze -> Silver)
│       ├── ml/         # Обучение моделей и инференс
│       ├── scraper/    # Сбор данных из Telegram (Telethon)
│       └── analytics/  # Скрипты для дашбордов
└── README.md

```

## Установка и Запуск

Все команды выполняются из корня репозитория.

### 1. Запуск контейнеров

```bash
# Сборка и запуск в фоновом режиме
docker-compose -f docker/docker-compose.yml up -d --build

```

### 2. Настройка прав доступа (Обязательно)

Для корректной записи данных в Hive необходимо выдать права на папку хранилища внутри контейнера.

**Mac / Linux / Windows (Git Bash):**

```bash
docker-compose -f docker/docker-compose.yml exec -u 0 spark-master chmod -R 777 /user/hive/warehouse

```

---

## Пайплайн обработки данных

Ниже приведены команды для ручного запуска этапов пайплайна.

> **Важно для пользователей Windows (Git Bash):** Обратите внимание на двойные слеши `//app/...` в путях к скриптам. Это предотвращает ошибку конвертации путей Git Bash'ем. На Mac/Linux это тоже будет работать корректно.

### Шаг 1: ETL (Bronze -> Silver)

Преобразование сырых JSON-файлов в формат Parquet и регистрация таблиц `silver_posts` и `silver_comments` в Hive.

```bash
docker-compose -f docker/docker-compose.yml exec -e JAVA_HOME=//usr/lib/jvm/java-17-openjdk-amd64 scraper_service python //app/src/app/etl/bronze_to_silver.py

```

### Шаг 2: Генерация Big Data (Synthetic Gold)

Генерация синтетических данных для нагрузочного тестирования и создания объема (миллионы строк).

```bash
docker-compose -f docker/docker-compose.yml exec -e JAVA_HOME=//usr/lib/jvm/java-17-openjdk-amd64 scraper_service python //app/src/app/etl/data_generator.py

```

### Шаг 3: Machine Learning (Inference)

Запуск модели Логистической Регрессии. Скрипт обучает модель на `dataset.csv` и классифицирует данные из слоя Silver. Результат сохраняется в Hive таблицу `gold_logistic_predictions`.

```bash
docker-compose -f docker/docker-compose.yml exec -e JAVA_HOME=//usr/lib/jvm/java-17-openjdk-amd64 scraper_service python //app/src/app/ml/train_log_regression_dataset.py

```

### Шаг 4: Проверка данных

Утилита для проверки наличия таблиц в Hive и подсчета строк.

```bash
docker-compose -f docker/docker-compose.yml exec -e JAVA_HOME=//usr/lib/jvm/java-17-openjdk-amd64 scraper_service python //app/src/app/etl/check_data.py

```

---

## Аналитика (Apache Superset)

1. **Инициализация:**
Выполните скрипт для создания администратора и загрузки дефолтных настроек.
```bash
bash scripts/init_superset.sh

```


2. **Вход в систему:**
Откройте браузер: [http://localhost:8088](https://www.google.com/search?q=http://localhost:8088)
* **Логин:** `admin`
* **Пароль:** `admin`


3. **Подключение к Hive:**
Используйте следующий SQLAlchemy URI для подключения базы данных:
```text
hive://hive-server:10000/default

```



## Управление контейнерами

**Перезапуск всех сервисов (с удалением старых):**

```bash
docker-compose -f docker/docker-compose.yml down
docker-compose -f docker/docker-compose.yml up -d

```

**Просмотр логов (например, Spark):**

```bash
docker-compose -f docker/docker-compose.yml logs -f spark-master

```