#!/bin/bash

# Настройки
CONTAINER="spark_master"
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 ЗАПУСК ПОЛНОГО ЦИКЛА (MANUAL PIPELINE)...${NC}"

# 1. ETL (Очистка и создание Silver слоя)
echo -e "\n🛠  [1/5] ETL: Bronze -> Silver..."
docker exec -it $CONTAINER python3 /app/etl/bronze_to_silver.py

# 2. Генерация данных (Создание Gold слоя для Big Data)
echo -e "\n💎 [2/5] DATA GEN: Генерация синтетических данных (x200)..."
docker exec -it $CONTAINER python3 /app/etl/data_generator.py

# 3. ML: Обучение моделей (Все 3 уровня)
echo -e "\n🧠 [3/5] ML: Обучение моделей..."

echo "   -> 3.1. Словарь (Baseline)..."
docker exec -it $CONTAINER python3 /app/ml/toxic_classifier.py

echo "   -> 3.2. Supervised (Medium Data)..."
docker exec -it $CONTAINER python3 /app/ml/supervised_job.py

echo "   -> 3.3. Big Data (FastText)..."
docker exec -it $CONTAINER python3 /app/ml/train_big_dataset.py

# 4. Аналитика (Дашборд)
echo -e "\n📊 [4/5] ANALYTICS: Генерация финального дашборда..."
docker exec -it $CONTAINER python3 /app/analytics/final_dashboard.py

echo -e "\n${GREEN}🏁 ВСЕ ГОТОВО! Отчеты сохранены в папке data/reports${NC}"