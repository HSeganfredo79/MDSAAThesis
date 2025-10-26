#!/bin/bash
source .venv/bin/activate

set -e  # Exit on error
set -o pipefail  # Exit on command failure in pipes

# Step 1: Verify Services
echo "🟢 Verifying Running Services..."
if systemctl is-active --quiet kafka; then
    echo "✅ Kafka is active."
else
    echo "❌ Kafka is not running correctly."
    exit 1
fi

if pgrep -f "kafka_producer.py" > /dev/null; then
    echo "✅ Kafka Producer is active."
else
    echo "❌ Kafka Producer is not running correctly."
    exit 1
fi

# Step 2: Start Verifier
echo "🟡 Starting Verifier for ingestion pipeline..."
python3 verify_producer_consumer.py

deactivate
