#!/bin/bash
set -e

echo "======================================="
echo "Initializing NameNode..."
echo "======================================="

NAMENODE_DIR="/opt/hadoop/data/nameNode"

if [ ! -d "$NAMENODE_DIR/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force -nonInteractive
else
    echo "NameNode already formatted."
fi

echo "Starting NameNode service..."
hdfs namenode
