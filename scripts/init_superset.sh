echo "[5/5] Запуск и настройка Superset..."
DOCKER_COMPOSE_FILE="docker/docker-compose.yml"
DC="docker-compose -f $DOCKER_COMPOSE_FILE"
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