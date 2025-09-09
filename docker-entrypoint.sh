#!/bin/bash
set -euo pipefail

echo "Entrypoint: preparing environment"

CSV_PATH="src/sql/source_data.csv"
HDFS_PATH="hdfs:///data/raw/source_data"

if [ -f "$CSV_PATH" ]; then
    echo "Found $CSV_PATH — uploading via Spark to $HDFS_PATH"
    spark-submit --master local[2] src/utils/uploader.py "$CSV_PATH" "$HDFS_PATH"
else
    echo "CSV file $CSV_PATH not found — skipping upload"
fi

echo "Starting lab7 datamart+model job"

spark-submit --master local[2] src/datamart/DataMart.scala "$HDFS_PATH"

echo "DataMart+Model job finished"
exit 0
