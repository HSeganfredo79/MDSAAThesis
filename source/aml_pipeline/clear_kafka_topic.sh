#!/bin/bash

# Configuration
KAFKA_BIN_PATH="/opt/kafka/bin"
BROKER="192.168.0.4:9092"
TOPIC="usdc-transactions"

# Function to delete and recreate topic
delete_and_recreate_topic() {
    echo "🟡 Deleting Kafka topic: $TOPIC"
    $KAFKA_BIN_PATH/kafka-topics.sh --bootstrap-server $BROKER --delete --topic $TOPIC

    # Wait a few seconds to ensure deletion is processed
    sleep 5

    echo "🟡 Recreating Kafka topic: $TOPIC"
    $KAFKA_BIN_PATH/kafka-topics.sh --bootstrap-server $BROKER \
        --create --topic $TOPIC --partitions 3 --replication-factor 1

    if [ $? -eq 0 ]; then
        echo "✅ Topic '$TOPIC' successfully recreated."
    else
        echo "❌ Failed to recreate topic '$TOPIC'. Please check Kafka logs."
    fi
}

# Execute the function
delete_and_recreate_topic
