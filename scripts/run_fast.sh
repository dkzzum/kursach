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

echo -e "${BLUE}📂 Работаем в: ${PROJECT_ROOT}${NC}"

# --- ШАГ 1: ПОДНЯТИЕ КОНТЕЙНЕРОВ ---
echo -e "\n${YELLOW}⚡ [1/2] Проверка/Запуск контейнеров (без билда)...${NC}"
docker-compose -f docker/docker-compose.yml up -d

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Ошибка Docker Compose.${NC}"
    exit 1
fi

# Небольшая пауза на случай, если контейнер был выключен и только просыпается
sleep 3

# --- ШАГ 2: ЗАПУСК ПАЙПЛАЙНА ---
echo -e "\n${GREEN}🚀 [2/2] Запуск Оркестратора...${NC}"
docker-compose -f docker/docker-compose.yml exec scraper_service python jobs/pipeline.py