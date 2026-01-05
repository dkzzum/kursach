# 🛡️ Big Data Pipeline: Анализ токсичности в Telegram

Проект реализует полный цикл обработки данных (End-to-End Data Engineering Pipeline) для выявления и анализа деструктивного контента в комментариях Telegram.

**Технологический стек:**
* **Вычисления:** Apache Spark 3.5 (PySpark)
* **Хранилище данных:** Apache Hive (Metastore) + PostgreSQL
* **Инфраструктура:** Docker & Docker Compose
* **ML:** Spark MLlib (Logistic Regression, TF-IDF)
* **Визуализация:** Matplotlib, Seaborn

---

## 📋 Предварительные требования

Перед запуском убедитесь, что у вас установлены:
1.  **Docker** и **Docker Compose**.
2.  Свободно минимум **4-6 ГБ RAM** (для работы Spark с большими данными).
3.  Исходные данные (JSON) должны лежать в папке `data/raw/posts` и `data/raw/comments`.

---

## 🚀 Быстрый старт (Пошаговая инструкция)

### Шаг 1: Запуск инфраструктуры

Сборка и запуск контейнеров (Spark Master, Workers, Hive Metastore, PostgreSQL).

```bash
docker-compose up -d --build
```
⏳ Важно: Подождите 30-60 секунд после запуска, чтобы базы данных (Hive Metastore DB) успели инициализироваться.

Проверьте статус контейнеров:

```Bash
docker-compose ps
```

Все контейнеры должны иметь статус Up.

Шаг 2: ETL-процесс (Сырые данные -> Data Warehouse)
2.1. Загрузка (Bronze -> Silver) Скрипт читает сырые JSON-файлы, очищает текст от мусора/ссылок, исправляет структуру и сохраняет в Hive (таблицы silver_posts, silver_comments).

```Bash
docker exec -it spark_master python3 /app/etl/bronze_to_silver.py
```

2.2. Генерация Big Data (Silver -> Gold) Для нагрузочного тестирования мы генерируем синтетический набор данных ("раздуваем" исходные данные в 200 раз), получая >2.5 млн записей.

```Bash
docker exec -it spark_master python3 /app/etl/data_generator.py
```

2.3. Проверка данных Убедимся, что таблицы созданы и заполнены.

```Bash
docker exec -it spark_master python3 /app/etl/check_data.py
```

Шаг 3: Машинное обучение (Machine Learning)
Мы обучаем три разные модели для сравнения эффективности и масштабируемости.

3.1. Метод 1: Поиск по словарю (Baseline) Простой поиск по списку плохих слов. Самый быстрый, но наименее точный метод.

```Bash
docker exec -it spark_master python3 /app/ml/toxic_classifier.py
```

3.2. Метод 2: Supervised Learning (Средняя нагрузка) Обучение на размеченном датасете (labeled.csv). Используется для теста масштабируемости (замер времени обучения при разном объеме данных).

```Bash
docker exec -it spark_master python3 /app/ml/supervised_job.py
```

3.3. Метод 3: Big Data Model (Максимальная точность) Обучение на большом корпусе текстов (dataset.txt, 250k+ строк) и применение к миллионам записей в Hive.

```Bash
docker exec -it spark_master python3 /app/ml/train_big_dataset.py
```

Шаг 4: Аналитика и Отчеты
Финальный этап. Скрипт собирает данные из всех таблиц Hive (silver_posts, gold_bigdata_predictions и др.) и строит графики.

```Bash
docker exec -it spark_master python3 /app/analytics/final_dashboard.py
```

📊 Где искать результаты? Графики автоматически сохраняются в папку data/reports/ на вашем компьютере:

- 1_activity_dynamics.png — Динамика постов по времени.

- 2_top_toxic_users.png — Топ-10 авторов токсичного контента.

- 3_destructive_ratio.png — Доля токсичности (Pie Chart).

- 4_model_comparison.png — Сравнение точности разных методов.

- 5_final_conclusion.png — Итоговая столбчатая диаграмма.

- 6_scalability_time.png — График времени обучения от объема данных.

🛠️ Полезные команды
Очистка проекта (Полный сброс) Если нужно удалить все данные из базы и начать заново:

```Bash
docker exec -it spark_master rm -rf /user/hive/warehouse/*
docker exec -it spark_master rm -rf /data/metastore_db
```

Ручной доступ к Spark SQL Посмотреть таблицы через консоль:

```Bash
docker exec -it spark_master pyspark
# Внутри Python:
spark.sql("SHOW TABLES").show()
```
