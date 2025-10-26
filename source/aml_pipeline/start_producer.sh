#!/bin/bash
# Activate virtual environment
source .venv/bin/activate

set -e  # Exit on error
set -o pipefail  # Exit on command failure in pipes

# Configuration
KAFKA_HOME="/opt/kafka"
SPARK_HOME="/opt/spark"
NEO4J_HOME="/var/lib/neo4j"
LOG_DIR="/var/log/aml_pipeline"

# Create log directory if missing
mkdir -p $LOG_DIR

# Step 1: Start Kafka
echo "🟡 Starting Kafka..."
sudo systemctl start kafka || {
  echo "❌ Failed to start Kafka. Exiting."
  exit 1
}

# Step 2: Create Kafka Topic (if missing)
# One partition per core
TOPIC="usdc-transactions"
echo "🟡 Checking for Kafka topic: $TOPIC"

if ! $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server 192.168.0.4:9092 | grep -q $TOPIC; then
    echo "✅ Creating Kafka topic: $TOPIC"
    $KAFKA_HOME/bin/kafka-topics.sh --create \
        --bootstrap-server 192.168.0.4:9092 \
        --replication-factor 1 \
        --partitions 8 \
        --topic $TOPIC
else
    echo "✅ Kafka topic '$TOPIC' already exists."
fi

# Step 3: Start Besu → Kafka Producer
echo "🟡 Starting Kafka Producer for USDC Transactions..."
#nice --1
nohup python3 kafka_producer.py >> $LOG_DIR/kafka_producer.log 2>&1 &

echo "✅ Kafka Producer started and logging to /var/log/aml_pipeline/kafka_producer.log"

deactivate
