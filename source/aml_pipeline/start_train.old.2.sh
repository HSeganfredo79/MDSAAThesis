#!/bin/bash
# Activate virtual environment
source .venv/bin/activate

set -e  # Exit on error
set -o pipefail  # Exit on command failure in pipes

# Configuration
LOG_DIR="/var/log/aml_pipeline"

echo "🟡 Starting training..."

# Change to working directory
cd /root/tese_henrique/aml_pipeline

# Start the consumer process and log output
nohup nice --1 python3 train.py "$1" >> $LOG_DIR/train.log 2>&1 &

echo "✅ Training started and logging to /var/log/aml_pipeline/train.log"

deactivate
