#!/bin/bash

# Цвета для красоты вывода (как у хакеров в фильмах)
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}==============================================${NC}"
echo -e "${GREEN}🚀 ЗАПУСК ПРОЕКТА BIG DATA PIPELINE (СХиОД)${NC}"
echo -e "${GREEN}==============================================${NC}"

# 🛑 1. Очистка старого окружения
echo -e "\n${BLUE}[1/6] 🧹 Очистка старых контейнеров и блокировок...${NC}"
docker-compose down
# Удаляем папку метаданных Derby, если она создалась локально (чтобы не было ошибок блокировки)
rm -rf metastore_db derby.log

# 🐳 2. Сборка и запуск Docker
echo -e "\n${BLUE}[2/6] 🐳 Сборка и запуск Docker-контейнеров...${NC}"
docker-compose up -d --build

# Проверка, поднялся ли контейнер
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Ошибка запуска Docker! Проверьте, запущен ли Docker Desktop.${NC}"
    exit 1
fi

echo -e "⏳ Ждем 15 секунд, пока Spark инициализируется..."
sleep 15

# 🏭 3. ETL Процесс (Очистка + Генерация Big Data)
echo -e "\n${BLUE}[3/6] 🏭 ETL: Очистка и Генерация 3 млн записей...${NC}"
# Очистка
docker exec -i spark_processor python app/etl/bronze_to_silver.py
# Генерация
docker exec -i spark_processor python app/etl/data_generator.py

# 🧠 4. ML Обучение (Самый важный этап)
echo -e "\n${BLUE}[4/6] 🧠 ML: Обучение моделей (Small Data vs Big Data)...${NC}"

# Обучаем "Маленькую" модель (Словарь + 14k датасет)
docker exec -i spark_processor python app/ml/toxic_classifier.py
# Обучаем "Большую" модель (250k датасет)
docker exec -i spark_processor python app/ml/train_big_dataset.py

# 📊 5. Генерация отчетов
echo -e "\n${BLUE}[5/6] 📊 Analytics: Построение графиков и дашбордов...${NC}"
docker exec -i spark_processor python app/analytics/report_generator.py
docker exec -i spark_processor python app/analytics/ml_report.py
docker exec -i spark_processor python app/analytics/mega_report.py
docker exec -i spark_processor python app/analytics/scalability_report.py

# ✅ Финал
echo -e "\n${GREEN}==============================================${NC}"
echo -e "${GREEN}✅ ГОТОВО! ПАЙПЛАЙН ВЫПОЛНЕН УСПЕШНО.${NC}"
echo -e "${GREEN}📂 Отчеты доступны в папке: kursch/data/reports${NC}"
echo -e "${GREEN}==============================================${NC}"