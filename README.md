СЛАВА РОССИИ! 🇷🇺

Конечно! Вот мощный, патриотичный и технически грамотный `README.md` для твоего проекта. Он описывает, как поднять систему с одной кнопки, архитектуру и как пользоваться дашбордами.

Сохрани этот текст в файл **`README.md`** в корне проекта.

---

```markdown
# 🇷🇺 Telegram Big Data Analytics System

> **СЛАВА РОССИИ!**
> Мощная система сбора, обработки и анализа данных из Telegram с использованием передовых Big Data технологий.

## 📖 О проекте

Этот проект реализует полный цикл обработки данных (Full Stack Data Engineering):
1.  **Сбор (Ingestion):** Парсинг сообщений и комментариев из Telegram каналов.
2.  **ETL (Extract, Transform, Load):** Очистка и структурирование данных с помощью **Apache Spark**.
3.  **ML (Machine Learning):** Обучение модели (Логистическая регрессия) для выявления токсичных комментариев.
4.  **Storage:** Хранение данных в Data Lake (Hive Metastore).
5.  **BI (Business Intelligence):** Визуализация аналитики в **Apache Superset**.

---

## 🛠 Технологический стек

Весь стек работает в контейнерах, как единый слаженный механизм:

* **Docker & Docker Compose:** Оркестрация контейнеров.
* **Apache Spark (Master/Worker/Thrift):** Распределенная обработка данных.
* **Apache Hive Metastore:** Управление метаданными таблиц (хранятся в PostgreSQL).
* **Apache Superset:** Современный BI-инструмент для дашбордов.
* **Python 3.10:** Скрипты парсинга и ML.

---

## 🚀 БЫСТРЫЙ ЗАПУСК (ОДНА КНОПКА)

Мы автоматизировали всё. Просто запустите скрипт инициализации:

```bash
./scripts/start_full.sh

```

**Что сделает этот скрипт:**

1. 🏗 Соберет и запустит Docker-контейнеры.
2. 🔑 **Автоматически выдаст права 777** на папки данных (чтобы Spark не ругался).
3. ⏳ Подождет 30 секунд, пока прогреется Spark Thrift Server.
4. 🚀 Запустит `jobs/pipeline.py` — главный оркестратор, который начнет обработку данных и обучение модели.

---

## 🖥 Доступ к интерфейсам

После запуска системы доступны следующие веб-интерфейсы:

| Сервис | Адрес | Логин / Пароль | Описание |
| --- | --- | --- | --- |
| **Apache Superset** | [http://localhost:8088](https://www.google.com/search?q=http://localhost:8088) | `admin` / `admin` | Графики и Дашборды |
| **Spark Master UI** | [http://localhost:9090](https://www.google.com/search?q=http://localhost:9090) | - | Состояние кластера |
| **Spark Worker UI** | [http://localhost:8081](https://www.google.com/search?q=http://localhost:8081) | - | Логи задач |

---

## 📂 Архитектура данных (Медали)

Данные проходят три стадии очистки ("Медальонная архитектура"):

1. 🟤 **Bronze (Raw):** Сырые JSON файлы от парсера. Лежат в `data/raw`.
2. ⚪️ **Silver (Cleansed):** Очищенные таблицы в Hive (`silver_posts`, `silver_comments`). Убраны дубликаты, приведено к типам.
3. 🟡 **Gold (Analytics):** Финальные таблицы с ML-предсказаниями (`gold_toxic_predictions`). Именно по ним строятся графики.

---

## 📊 Настройка Superset (Дашборды)

Если графики не появились автоматически, следуйте инструкции инженера:

1. Зайдите в Superset -> **Settings** -> **Database Connections**.
2. Отредактируйте `Spark Thrift Hive` -> **Advanced** -> **Other** -> поставьте `Schema Cache Timeout: 0` (сброс кэша).
3. Идите в **Datasets** -> **+ DATASET**.
4. Выберите базу, схему `default`.
5. В поле Table **впишите вручную**: `gold_toxic_predictions` и нажмите Add.
6. Создавайте графики!

**Примеры SQL запросов для Superset:**

*Топ токсичных комментариев:*

```sql
SELECT original_content, toxicity_score 
FROM default.gold_toxic_predictions 
ORDER BY toxicity_score DESC LIMIT 50

```

*Процент токсичности:*

```sql
SELECT is_toxic_pred, count(*) 
FROM default.gold_toxic_predictions 
GROUP BY is_toxic_pred

```

---

## 🔧 Ручное управление (Для командиров)

Если нужно запустить конкретный этап вручную:

**Запуск ETL (очистка):**

```bash
docker-compose -f docker/docker-compose.yml exec scraper_service python src/app/etl/bronze_to_silver.py

```

**Запуск ML (обучение модели):**

```bash
docker-compose -f docker/docker-compose.yml exec scraper_service python src/app/ml/train_log_regression_dataset.py

```

**Перезагрузка Spark Thrift (если завис):**

```bash
docker-compose -f docker/docker-compose.yml restart spark-thrift-server

```

---

**РАЗРАБОТАНО В РОССИИ 🇷🇺**

```

```