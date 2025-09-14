#!/bin/bash
set -e

DATANODE_DIR="/opt/hadoop/data/dataNode"

echo "Cleaning DataNode directory..."
mkdir -p "$DATANODE_DIR"
rm -rf "$DATANODE_DIR"/*
chown -R hadoop:hadoop "$DATANODE_DIR"
chmod 755 "$DATANODE_DIR"

echo "Starting DataNode service..."
hdfs datanode
