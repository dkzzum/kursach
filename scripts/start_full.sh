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
echo -e "\n${YELLOW}🔨 [1/4] Сборка и запуск контейнеров...${NC}"
docker-compose -f docker/docker-compose.yml up -d --build

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Ошибка Docker Compose.${NC}"
    exit 1
fi

# --- ШАГ 2: НАСТРОЙКА ПРАВ (ТОТ САМЫЙ FIX) ---
echo -e "\n${YELLOW}🔑 [2/4] Настройка прав доступа (Hive & Spark)...${NC}"
# Мы используем spark-master как "root-агента" чтобы выдать права на общие тома
docker-compose -f docker/docker-compose.yml exec -u root spark-master chmod -R 777 /user/hive/warehouse
docker-compose -f docker/docker-compose.yml exec -u root spark-master chmod -R 777 /data
echo -e "✅ Права доступа обновлены."

# --- ШАГ 3: ОЖИДАНИЕ ГОТОВНОСТИ ---
echo -e "\n${YELLOW}⏳ [3/4] Ожидание инициализации сервисов...${NC}"
for i in {15..1}; do
    echo -ne "Осталось: $i сек... \r"
    sleep 1
done
echo -e "✅ Сервисы готовы.                   "

# --- ШАГ 4: ЗАПУСК ПАЙПЛАЙНА ---
echo -e "\n${GREEN}🚀 [4/4] Запуск Оркестратора...${NC}"
docker-compose -f docker/docker-compose.yml exec scraper_service python jobs/pipeline.py