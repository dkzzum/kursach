#!/bin/bash
echo "🚀 ЗАПУСК ПОЛНОГО ЦИКЛА (PIPELINE)..."

# 1. ETL (Очистка)
docker exec -i spark_processor python app/etl/bronze_to_silver.py

# 2. Генерация данных (Симуляция Big Data)
docker exec -i spark_processor python app/etl/data_generator.py

# 3. ML Анализ (Обучение и классификация)
docker exec -i spark_processor python app/ml/toxic_classifier.py

# 4. Генерация всех отчетов
docker exec -i spark_processor python app/analytics/report_generator.py
docker exec -i spark_processor python app/analytics/ml_report.py
docker exec -i spark_processor python app/analytics/mega_report.py
docker exec -i spark_processor python app/analytics/scalability_report.py

echo "🏁 ВСЕ ГОТОВО! Проверьте папку data/reports"