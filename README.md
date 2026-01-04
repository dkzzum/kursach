# Инструкция по запуску
## 1. Предварительная настройка
Перед запуском убедитесь, что в файле core/config.py указаны ваши актуальные данные от Telegram API (api_id, api_hash).

## 2. Запуск инфраструктуры (Docker)
Сборка образа и запуск контейнера со Spark и Java. Выполните в терминале (в корне проекта):

```Bash
docker-compose up -d --build
```
Что происходит: Скачивается образ Spark, устанавливается Java, поднимается контейнер spark_processor и пробрасываются папки для данных (raw_data, spark-warehouse).

## 3. Сбор данных (Ingestion / Bronze Layer)
Запуск парсера для скачивания постов и комментариев из Telegram. Скрипт выполняется локально на вашем компьютере, но сохраняет данные в папку, доступную Докеру.

```Bash
python main.py
```
Что происходит: Скрипт подключается к Telegram, скачивает сообщения и сохраняет их в папку ./raw_data в формате JSON.

## 4. Обработка данных (ETL / Silver Layer)
Запуск Spark-скрипта внутри контейнера. Он читает JSON, чистит данные, удаляет дубликаты и создает управляемые таблицы Hive.

```bash
docker exec -it spark_processor python etl_bronze_to_silver.py
```
Что происходит:

Читаются "грязные" JSON файлы.

Удаляются HTML-теги и спецсимволы.

Данные конвертируются в формат Parquet.

Создаются таблицы silver_posts и silver_comments во внутреннем Hive Metastore.

## 5. Проверка результатов
Запуск скрипта проверки, чтобы убедиться, что таблицы создались и данные доступны через SQL.

```Bash
docker exec -it spark_processor python check_silver.py
```
Ожидаемый результат: Вывод схемы таблиц и первых 5 строк очищенных данных в консоль.

# 🛠 Дополнительные команды
Ручной вход в консоль PySpark (SQL): Если вы хотите писать SQL-запросы к таблицам вручную:

```Bash
docker exec -it spark_processor pyspark --driver-java-options "-Dderby.system.home=/tmp/derby"
```

Пример запроса внутри: spark.sql("SELECT * FROM silver_posts LIMIT 10").show()

Полная очистка (Сброс базы): Если нужно удалить базу данных метаданных и начать с нуля (сами JSON файлы останутся):

```Bash
# 1. Остановить контейнер
docker-compose down

# 2. Удалить папки метаданных и склада
rm -rf metastore_db
rm -rf spark-warehouse

# 3. Запустить заново
docker-compose up -d
```