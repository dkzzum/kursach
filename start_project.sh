#!/bin/bash

# Цвета
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}==============================================${NC}"
echo -e "${GREEN}🚀 ЗАПУСК ENTERPRISE DATA PIPELINE (СХиОД)${NC}"
echo -e "${GREEN}==============================================${NC}"

# 🛑 1. Очистка
echo -e "\n${BLUE}[1/7] 🧹 Очистка старых контейнеров и метаданных...${NC}"
docker-compose down
rm -rf metastore_db derby.log

# 🐳 2. Docker
echo -e "\n${BLUE}[2/7] 🐳 Сборка и запуск инфраструктуры...${NC}"
docker-compose up -d --build

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Ошибка Docker! Проверьте, запущен ли Docker Desktop.${NC}"
    exit 1
fi

echo -e "⏳ Инициализация Spark (ждем 10 сек)..."
sleep 10

# 🏭 3. ETL (Первичная загрузка)
echo -e "\n${BLUE}[3/7] 🏭 ETL: Загрузка исторических данных и генерация Big Data...${NC}"
docker exec -i spark_processor python app/etl/bronze_to_silver.py
docker exec -i spark_processor python app/etl/data_generator.py

# 🧠 4. ML (Обучение)
echo -e "\n${BLUE}[4/7] 🧠 ML: Обучение эталонных моделей...${NC}"
docker exec -i spark_processor python app/ml/toxic_classifier.py
docker exec -i spark_processor python app/ml/train_big_dataset.py

# 📊 5. Отчеты
echo -e "\n${BLUE}[5/7] 📊 Analytics: Генерация первичных отчетов...${NC}"
docker exec -i spark_processor python app/analytics/report_generator.py
docker exec -i spark_processor python app/analytics/ml_report.py
docker exec -i spark_processor python app/analytics/mega_report.py
docker exec -i spark_processor python app/analytics/scalability_report.py

# 📦 6. Проверка зависимостей хоста (для планировщика)
echo -e "\n${BLUE}[6/7] 📦 Проверка окружения Python (Host)...${NC}"
# Проверяем, установлен ли schedule, если нет - ставим
pip3 show schedule > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}⚠️ Библиотека 'schedule' не найдена. Устанавливаем...${NC}"
    pip3 install schedule pyrogram tgcrypto
else
    echo -e "✅ Необходимые библиотеки найдены."
fi

# ⏰ 7. Запуск КОНВЕЙЕРА
echo -e "\n${GREEN}==============================================${NC}"
echo -e "${GREEN}✅ БАЗОВАЯ НАСТРОЙКА ЗАВЕРШЕНА.${NC}"
echo -e "${GREEN}🔄 ПЕРЕХОД В РЕЖИМ АВТОМАТИЧЕСКОГО КОНВЕЙЕРА...${NC}"
echo -e "${YELLOW}(Нажмите Ctrl+C, чтобы остановить планировщик)${NC}"
echo -e "${GREEN}==============================================${NC}"

# Запускаем планировщик и передаем ему управление
python3 scheduler.py