#!/bin/bash
set -euo pipefail

echo "Entrypoint: preparing environment"

HDFS_INPUT="hdfs:///app/sql/source_data.csv"

echo "Step 1: Running Scala ETL job and passing to Python"
spark-submit --master local[2] target/scala-2.12/datamartproject_2.12-0.1.jar \
"$HDFS_INPUT" | python3 src/main.py

echo "All jobs finished"
exit 0
