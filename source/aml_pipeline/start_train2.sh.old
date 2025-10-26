#!/bin/bash
source .venv/bin/activate
set -e
set -o pipefail

LOG_DIR="/var/log/aml_pipeline"
mkdir -p "$LOG_DIR"

echo "🟡 Starting training v2..."
cd /root/tese_henrique/aml_pipeline

nohup nice --1 python3 train2.py "$1" >> $LOG_DIR/train2.log 2>&1 &

echo "✅ Training v2 started and logging to $LOG_DIR/train2.log"
deactivate
