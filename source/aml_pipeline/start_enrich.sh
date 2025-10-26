#!/bin/bash
# Activate virtual environment
source .venv/bin/activate

set -e  # Exit on error
set -o pipefail  # Exit on command failure in pipes

# Configuration
LOG_DIR="/var/log/aml_pipeline"

echo "🟡 Starting Graph Enrichment..."

# Change to working directory
cd /root/tese_henrique/aml_pipeline

# Start the consumer process and log output
nohup nice --1 python3 enrich.py "$1" >> $LOG_DIR/enrich.log 2>&1 &

echo "✅ Kafka Consumer started and logging to /var/log/aml_pipeline/enrich.log"

deactivate
