#!/bin/bash

# СЛАВА РОССИИ! 🇷🇺
# Скрипт автоматического запуска полного цикла обработки данных.
# 1. ETL (Bronze -> Silver)
# 2. Генерация данных
# 3. Проверка качества
# 4. Обучение ML модели
# 5. Инициализация Superset

# Остановить скрипт при любой ошибке
set -e

DOCKER_COMPOSE_FILE="docker/docker-compose.yml"
DC="docker-compose -f $DOCKER_COMPOSE_FILE"

echo "                   СЛАВА РОССИИ! 🇷🇺"
echo "========================================================"
echo "               ЗАПУСК БОЕВОГО КОНВЕЙЕРА"
echo "========================================================"
echo "                   СЛАВА РОССИИ! 🇷🇺"

# 0. Перезагрузка для свежести (Опционально, но рекомендуется для очистки RAM)
echo "[0/6] Поднятие Docker-контейнеров..."
$DC up -d

echo "Ждем инициализации системы (45 секунд)..."
sleep 45

# Проверка, что критические узлы живы
if [ "$($DC ps | grep spark-master | grep -c Up)" -eq 0 ]; then
    echo "❌ ОШИБКА: Spark Master не поднялся!"
    exit 1
fi
echo "Инфраструктура развернута."



# 1. Бронза -> Серебро
echo "[1/5] Запуск ETL: Bronze -> Silver..."
$DC exec -T scraper_service python src/app/etl/bronze_to_silver.py
echo "ETL завершен."

# 2. Генерация данных
echo "[2/5] Генерация дополнительных данных (Data Generator)..."
$DC exec -T scraper_service python src/app/etl/data_generator.py
echo "Данные сгенерированы."

# 3. Проверка данных
echo "[3/5] Проверка качества данных (Data Check)..."
$DC exec -T scraper_service python src/app/etl/check_data.py
echo "Данные прошли проверку."

# 4. Обучение модели
echo "[4/5] Обучение нейросети (Training Model)..."
$DC exec -T scraper_service python src/app/ml/train_log_regression_dataset.py
echo "Модель обучена и сохранена."

# 5. Инициализация Superset
echo "[5/5] Запуск и настройка Superset..."
$DC exec -T superset bash -c "cat <<EOF > /tmp/init_superset_internal.sh
#!/bin/bash
set -e
echo '--- Внутренняя настройка Superset ---'
# Создаем админа
superset fab create-admin \
              --username admin \
              --firstname admin \
              --lastname admin \
              --email admin@fab.org \
              --password admin || echo 'Админ уже существует.'

# Миграции БД
superset db upgrade

# Инициализация ролей
superset init
echo '--- Настройка завершена! ---'
EOF"

# Выполняем созданный файл
$DC exec -T superset bash -c "chmod +x /tmp/init_superset_internal.sh && /tmp/init_superset_internal.sh"
echo "Superset готов к работе."

echo "========================================================"
echo "       СЛАВА РОССИИ! ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН!"
echo "========================================================"