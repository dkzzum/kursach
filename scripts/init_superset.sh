#!/bin/bash

echo "Настройка Apache Superset..."

# 1. Создаем админа (если уже есть, выдаст ошибку, это нормально)
docker-compose -f docker/docker-compose.yml exec superset superset fab create-admin \
              --username admin \
              --firstname Admin \
              --lastname User \
              --email admin@fab.org \
              --password admin \
              2>/dev/null || true

# 2. Обновляем структуру внутренней БД Superset
docker-compose -f docker/docker-compose.yml exec superset superset db upgrade

# 3. Инициализируем роли и права
docker-compose -f docker/docker-compose.yml exec superset superset init

echo "Superset готов: http://localhost:8088 (admin/admin)"