# Telegram Toxicity Analyzer (Big Data & ML)

Проект по автоматизированному сбору, обработке и анализу токсичности комментариев в Telegram-каналах с использованием стека технологий Big Data.

## 🛠 Технологический стек

* **Infrastructure**: Docker, Docker Compose
* **Data Processing**: Apache Spark (PySpark)
* **Storage**: Hive Metastore, PostgreSQL (Metastore DB), Parquet
* **ML**: Spark MLlib (Logistic Regression)
* **Visualization**: Matplotlib, Seaborn

---

## 🏗 Архитектура данных

Проект следует архитектуре **Medallion**:

1. **Bronze**: Сырые JSON-данные из Telegram (скрепер).
2. **Silver**: Очищенные данные в формате Parquet (удаление дублей, типизация).
3. **Gold**: Размеченные данные с предсказаниями модели токсичности.

---

## 🚀 Запуск проекта

### 1. Подготовка окружения

Убедитесь, что у вас установлены Docker и Docker Compose. Создайте необходимые папки для логов:

```bash
mkdir -p spark_events data/reports

```

### 2. Сборка и запуск контейнеров

Запустите всю инфраструктуру (PostgreSQL, Hive, Spark Master, Spark Worker, Scraper):

```bash
docker-compose -f docker/docker-compose.yml up -d --build

```

### 3. Исправление прав доступа (Важно!)

Для корректной работы Hive и Spark необходимо дать права на запись в общие тома:

```bash
docker-compose -f docker/docker-compose.yml exec -u root spark-master chmod -R 777 /user/hive/warehouse
docker-compose -f docker/docker-compose.yml exec -u root spark-master chmod -R 777 /data

```

---

## 🔄 Пайплайн обработки

### Шаг 1: ETL (из Bronze в Silver)

Преобразование сырых JSON-файлов постов и комментариев в очищенные Parquet-файлы:

```bash
docker-compose -f docker/docker-compose.yml exec scraper_service python src/app/etl/bronze_to_silver.py

```

*Результат*: Файлы в `data/silver/posts` и `data/silver/comments`.

### Шаг 2: Machine Learning (из Silver в Gold)

Обучение модели Logistic Regression на размеченном сете и классификация собранных комментариев (650k+ записей):

```bash
docker-compose -f docker/docker-compose.yml exec scraper_service python src/app/ml/train_log_regression_dataset.py

```

*Результат*: Размеченные данные в `data/gold/predictions`.

### Шаг 3: Генерация отчетов

Создание графиков частотного анализа токсичной лексики:

```bash
docker-compose -f docker/docker-compose.yml exec scraper_service python src/app/analytics/dashboard_lr.py

```

*Результат*: График `data/reports/toxic_words_chart.png`.

---

## 📊 Мониторинг

* **Spark Master UI**: [http://localhost:9090](https://www.google.com/search?q=http://localhost:9090)
* **Spark History Server**: [http://localhost:18080](https://www.google.com/search?q=http://localhost:18080)
* **Spark Worker UI**: [http://localhost:8081](https://www.google.com/search?q=http://localhost:8081)

---

## ⚠️ Устранение неполадок

* **Ошибка `VERSION is obsolete**`: Игнорируйте, это предупреждение новой версии Docker Compose.
* **Ошибка `AnalysisException: [id] cannot be resolved**`: Проверьте маппинг колонок в ETL. Для постов это `post_id`, для комментариев — `comment_id`.
* **Ошибка `Hive registration skipped**`: Если данные в папках `data/` появились, значит ETL прошел успешно. Ошибка регистрации метаданных не блокирует ML-процесс.