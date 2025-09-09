#!/bin/bash
set -euo pipefail

echo "Entrypoint: preparing environment"

# If source CSV exists, try to upload to HDFS
CSV_PATH="src/sql/source_data.csv"
if [ -f "$CSV_PATH" ]; then
    echo "Found $CSV_PATH — attempting to copy to HDFS at /data/raw/source_data.csv"
    if command -v hdfs >/dev/null 2>&1; then
        hdfs dfs -mkdir -p /data/raw || true
        if hdfs dfs -put -f "$CSV_PATH" /data/raw/source_data.csv 2>/dev/null; then
            echo "Uploaded $CSV_PATH to hdfs:///data/raw/source_data.csv"
        else
            echo "Warning: failed to put $CSV_PATH to HDFS (put command returned non-zero)"
        fi
    else
        echo "Warning: 'hdfs' command not found in PATH — skipping HDFS upload"
    fi
else
    echo "CSV file $CSV_PATH not found — skipping HDFS upload"
fi

echo "Starting lab7 datamart+model job"

# Run the Scala DataMart which will preprocess and run the model
spark-submit --master local[2] src/datamart/DataMart.scala hdfs:///data/raw/source_data.csv

echo "DataMart+Model job finished"
exit 0