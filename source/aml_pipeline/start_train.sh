
#!/bin/bash
# start_train.sh — accepts ONE argument: either YYYYMMDD-YYYYMMDD or <days_back_int>
# Examples:
#   ./start_train.sh 20250401-20250430
#   ./start_train.sh 30
source .venv/bin/activate
set -e
set -o pipefail

LOG_DIR="/var/log/aml_pipeline"
mkdir -p "$LOG_DIR"

echo "🟡 Starting training (range/days-back)..."
cd /root/tese_henrique/aml_pipeline

nohup nice --1 python3 train.py "$1" >> "$LOG_DIR/train.log" 2>&1 &

echo "✅ Training started and logging to $LOG_DIR/train.log"
deactivate
