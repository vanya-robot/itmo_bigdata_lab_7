#!/bin/bash
set -euo pipefail

echo "Entrypoint: starting Spark data pipeline"

# Загружаем переменные из .env
if [ -f .env ]; then
    export $(cat .env | grep -v '#' | xargs)
fi

# Загружаем CSV в HDFS
echo "Step 0: Uploading CSV to HDFS"
python3 src/utils/uploader.py

# Используем переменные из .env
HDFS_INPUT_PATH_SCALA="$HDFS_RAW_PATH_SCALA"
SPARK_MASTER="$SPARK_MASTER"  
JAR_PATH="$JAR_PATH"
HDFS_PROCESSED_PATH="$HDFS_PROCESSED_PATH"

echo "Using HDFS path: $HDFS_INPUT_PATH_SCALA"
echo "Using Spark master: $SPARK_MASTER"

# 1. Запускаем Scala для кэширования
echo "Step 1: Running Scala ETL job to cache data in CSV"
spark-submit \
    --master "$SPARK_MASTER" \
    --class DataMart \
    "$JAR_PATH" \
    "$HDFS_INPUT_PATH_SCALA" \
    "$HDFS_PROCESSED_PATH" &

SCALA_PID=$!
echo "Scala process started with PID: $SCALA_PID"

# 2. Даем время на кэширование
sleep 15

# 3. Запускаем Python обработку
echo "Step 2: Running Python processing from CSV"
python3 src/main.py

# 4. Завершаем Scala процесс
kill $SCALA_PID 2>/dev/null || true

echo "All jobs finished successfully"
exit 0
