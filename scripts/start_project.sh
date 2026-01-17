#!/bin/bash

# Цвета
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color
CONTAINER="spark_master"

# Сохраняем путь к корню скриптов, чтобы возвращаться
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${GREEN}==============================================${NC}"
echo -e "${GREEN}🚀 ЗАПУСК ENTERPRISE DATA PIPELINE (СХиОД)${NC}"
echo -e "${GREEN}==============================================${NC}"

# 🛑 1. Очистка и запуск Docker
echo -e "\n${BLUE}[1/7] 🧹 Очистка и запуск инфраструктуры...${NC}"

# ИЗМЕНЕНИЕ: Переходим в папку docker, где лежит docker-compose.yml
cd "$PROJECT_ROOT/docker" || exit 1

docker-compose down
# Удаляем старые логи, если есть (они могут быть в корне или в docker)
rm -rf metastore_db derby.log

echo -e "\n${BLUE}[2/7] 🐳 Сборка и запуск контейнеров...${NC}"
docker-compose up -d --build

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Ошибка Docker! Проверьте, запущен ли Docker Desktop.${NC}"
    exit 1
fi

# Возвращаемся в корень, чтобы пути были предсказуемыми
cd "$PROJECT_ROOT"

echo -e "⏳ Инициализация Spark и Hive (ждем 30 сек)..."
sleep 30

# 🏭 3. ETL (Первичная загрузка)
echo -e "\n${BLUE}[3/7] 🏭 ETL: Загрузка исторических данных и генерация Big Data...${NC}"
# ИЗМЕНЕНИЕ: Пути src/app/...
docker exec -it $CONTAINER python3 src/app/etl/bronze_to_silver.py
docker exec -it $CONTAINER python3 src/app/etl/data_generator.py

# 🧠 4. ML (Обучение всех моделей)
echo -e "\n${BLUE}[4/7] 🧠 ML: Обучение эталонных моделей...${NC}"
docker exec -it $CONTAINER python3 src/app/ml/train_big_dataset.py

# 📊 5. Отчеты
echo -e "\n${BLUE}[5/7] 📊 Analytics: Генерация финальных отчетов...${NC}"
docker exec -it $CONTAINER python3 src/app/analytics/final_dashboard.py

# 📦 6. Проверка зависимостей хоста (для планировщика)
echo -e "\n${BLUE}[6/7] 📦 Проверка окружения Python (Host)...${NC}"
# Проверяем библиотеку schedule на хост-машине
pip3 show schedule > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}⚠️ Библиотека 'schedule' не найдена. Устанавливаем...${NC}"
    pip3 install schedule
else
    echo -e "✅ Необходимые библиотеки найдены."
fi

 ⏰ 7. Запуск планировщика
echo -e "\n${BLUE}[7/7] ⏰ Запуск планировщика задач (Scheduler)...${NC}"
echo -e "ℹ️  Планировщик будет запускать ETL каждые 2 часа."

# ИЗМЕНЕНИЕ: Scheduler теперь лежит в папке jobs
# Убиваем старые процессы scheduler.py, если есть
pkill -f "python3 jobs/scheduler.py" > /dev/null 2>&1

nohup python3 jobs/scheduler.py > scheduler.log 2>&1 &

echo -e "${GREEN}✅ ПРОЕКТ УСПЕШНО ЗАПУЩЕН!${NC}"
echo -e "   📌 Дашборд доступен: http://localhost:8050"
echo -e "   📌 Spark UI: http://localhost:8080"
echo -e "   📌 Логи планировщика: tail -f scheduler.log"