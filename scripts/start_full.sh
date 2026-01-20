#!/bin/bash

# --- НАСТРОЙКИ ЦВЕТОВ ---
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}🇷🇺 СЛАВА РОССИИ! Запускаем систему...${NC}"

# --- 1. ПЕРЕХОД В КОРЕНЬ ---
cd "$(dirname "$0")/.."

# --- 2. СБОРКА И ЗАПУСК ---
echo -e "\n${YELLOW}🔨 [1/4] Поднимаем инфраструктуру...${NC}"
docker-compose -f docker/docker-compose.yml up -d --build

if [ $? -ne 0 ]; then
    echo -e "${RED} Ошибка Docker Compose.${NC}"
    exit 1
fi

# --- 3. АВТО-ФИКС ПРАВ (ТО, ЧТО ТЫ ДЕЛАЛ РУКАМИ) ---
echo -e "\n${YELLOW} [2/4] Выдаем права на данные...${NC}"
echo "Ждем 5 секунд инициализации контейнеров..."
sleep 5
# Используем scraper_service (он легкий и запущен от root по умолчанию, либо spark-master)
docker-compose -f docker/docker-compose.yml exec -u root scraper_service chmod -R 777 /data /user/hive/warehouse
if [ $? -eq 0 ]; then
    echo -e "${GREEN} Права выданы успешно.${NC}"
else
    echo -e "${RED} Не удалось выдать права (возможно, контейнер еще не готов).${NC}"
fi

# --- 4. ОЖИДАНИЕ ГОТОВНОСТИ ---
echo -e "\n${YELLOW} [3/4] Ждем прогрева Spark Thrift Server (30 сек)...${NC}"
# Мы знаем, что в конфиге стоит sleep 30, так что просто визуализируем ожидание
for i in {30..1}; do
    echo -ne "Осталось: $i сек... \r"
    sleep 1
done
echo -e "${GREEN} Сервера готовы к бою.${NC}           "

# --- 5. ЗАПУСК ПАЙПЛАЙНА ---
echo -e "\n${GREEN} [4/4] Запускаем автоматический Пайплайн...${NC}"
# Запускаем в "detached" режиме (фоном), чтобы не блокировать терминал,
# или убираем -d если хочешь смотреть логи здесь.
docker-compose -f docker/docker-compose.yml exec -d scraper_service python jobs/pipeline.py

echo -e "\n${BLUE} Система работает!${NC}"
echo -e " Superset: http://localhost:8088 (admin/admin)"
echo -e " Spark UI: http://localhost:9090"
echo -e " Логи пайплайна: docker logs -f telegram_scraper"