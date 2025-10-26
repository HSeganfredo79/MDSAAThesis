#!/bin/bash
# Activate virtual environment
source .venv/bin/activate

set -e  # Exit on error
set -o pipefail  # Exit on command failure in pipes

# Configuration
LOG_DIR="/var/log/aml_pipeline"

echo "🟡 Starting Kafka Consumer for Neo4j Ingestion..."

# Change to working directory
cd /root/tese_henrique/aml_pipeline

# Start the consumer process and log output
# nice --1
nohup python3 kafka_consumer.py >> $LOG_DIR/kafka_consumer.log 2>&1 &

echo "✅ Kafka Consumer started and logging to /var/log/aml_pipeline/kafka_consumer.log"

deactivate
