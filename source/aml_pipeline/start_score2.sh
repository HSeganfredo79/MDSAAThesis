#!/bin/bash
# Activate virtual environment
source .venv/bin/activate

set -e
set -o pipefail

LOG_DIR="/var/log/aml_pipeline"
mkdir -p "$LOG_DIR"

echo "🟡 Starting Scoring v2..."

# Change to working directory
cd /root/tese_henrique/aml_pipeline

# --- helpers ---
is_yyyymmdd() {
  [[ "$1" =~ ^[0-9]{8}$ ]] || return 1
  # Validate real date (GNU date)
  date -u -d "${1:0:4}-${1:4:2}-${1:6:2}" +%Y%m%d >/dev/null 2>&1
}

next_day() {
  date -u -d "${1:0:4}-${1:4:2}-${1:6:2} +1 day" +%Y%m%d
}

usage() {
  echo "Usage:"
  echo "  $0 YYYYMMDD"
  echo "  $0 YYYYMMDD-YYYYMMDD"
  deactivate
  exit 1
}

[[ $# -ge 1 ]] || usage
ARG="$1"

if [[ "$ARG" == *"-"* ]]; then
  # ---------- Range mode ----------
  START="${ARG%-*}"
  END="${ARG#*-}"

  is_yyyymmdd "$START" || { echo "❌ Invalid start date: $START (YYYYMMDD)"; deactivate; exit 2; }
  is_yyyymmdd "$END"   || { echo "❌ Invalid end date: $END (YYYYMMDD)";   deactivate; exit 2; }
  if [[ "$START" > "$END" ]]; then tmp="$START"; START="$END"; END="$tmp"; fi

  echo "📆 Score v2 sequentially from $START to $END (inclusive)..."

  CUR="$START"
  while true; do
    echo "▶️  $CUR : launching score2.py (logs → $LOG_DIR/score2.log)"
    nohup nice --1 python3 score2.py "$CUR" >> "$LOG_DIR/score2.log" 2>&1 &
    pid=$!
    wait "$pid"
    rc=$?
    if [[ $rc -ne 0 ]]; then
      echo "🛑 $CUR : FAILED rc=$rc (see $LOG_DIR/score2.log)"
      deactivate
      exit $rc
    fi
    [[ "$CUR" == "$END" ]] && break
    CUR="$(next_day "$CUR")"
  done

  echo "🎉 Range completed. See $LOG_DIR/score2.log"

else
  # ---------- Single-day mode ----------
  DATE="$ARG"
  is_yyyymmdd "$DATE" || { echo "❌ Invalid date: $DATE (expected YYYYMMDD)"; deactivate; exit 2; }

  nohup nice --1 python3 score2.py "$DATE" >> "$LOG_DIR/score2.log" 2>&1 &
  echo "✅ Score v2 started for $DATE and logging to $LOG_DIR/score2.log"
fi

deactivate
