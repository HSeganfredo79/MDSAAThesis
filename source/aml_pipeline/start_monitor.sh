#!/bin/bash
# Activate virtual environment
source .venv/bin/activate

set -e
set -o pipefail

LOG_DIR="/var/log/aml_pipeline"
mkdir -p "$LOG_DIR"

echo "🟡 Starting AML drift monitoring..."

cd /root/tese_henrique/aml_pipeline

# Run the monitor in foreground with logging and slight CPU niceness
nohup nice -n 1 python3 monitor.py "$1" >> $LOG_DIR/monitor.log 2>&1 &

echo "✅ Drift monitor running and logging to $LOG_DIR/monitor.log"

deactivate
