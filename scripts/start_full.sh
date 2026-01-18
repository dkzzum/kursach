#!/bin/bash

# --- НАСТРОЙКИ ЦВЕТОВ ---
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# --- ОПРЕДЕЛЕНИЕ КОРНЯ ---
cd "$(dirname "$0")/.."
PROJECT_ROOT=$(pwd)

echo -e "${BLUE}📂 Корневая директория: ${PROJECT_ROOT}${NC}"

# --- ШАГ 1: СБОРКА И ЗАПУСК ---
echo -e "\n${YELLOW}🔨 [1/5] Сборка и запуск контейнеров...${NC}"
docker-compose -f docker/docker-compose.yml up -d --build

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Ошибка Docker Compose.${NC}"
    exit 1
fi

# --- ШАГ 2: НАСТРОЙКА ПРАВ ---
echo -e "\n${YELLOW}🔑 [2/5] Настройка прав доступа (Hive & Spark)...${NC}"
docker-compose -f docker/docker-compose.yml exec -u root spark-master chmod -R 777 /user/hive/warehouse
docker-compose -f docker/docker-compose.yml exec -u root spark-master chmod -R 777 /data
echo -e "✅ Права доступа обновлены."

# --- ШАГ 3: ОЖИДАНИЕ ГОТОВНОСТИ ---
echo -e "\n${YELLOW}⏳ [3/5] Ожидание инициализации сервисов...${NC}"
# Увеличили время до 25 сек, так как Superset долго грузится
for i in {25..1}; do
    echo -ne "Осталось: $i сек... \r"
    sleep 1
done
echo -e "✅ Сервисы готовы.                   "

# --- ШАГ 4: ИНИЦИАЛИЗАЦИЯ SUPERSET ---
echo -e "\n${YELLOW}📊 [4/5] Инициализация Superset...${NC}"
./scripts/init_superset.sh

# --- ШАГ 5: ЗАПУСК ПАЙПЛАЙНА ---
echo -e "\n${GREEN}🚀 [5/5] Запуск Оркестратора...${NC}"
docker-compose -f docker/docker-compose.yml exec scraper_service python jobs/pipeline.py