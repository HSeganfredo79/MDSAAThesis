#!/bin/bash
# daily_or_range_score.sh
# Usage:
#   ./daily_or_range_score.sh 20250401
#   ./daily_or_range_score.sh 20250401-20250430

set -euo pipefail

# --- Config ---
PROJECT_DIR="/root/tese_henrique/aml_pipeline"
VENV_DIR="$PROJECT_DIR/.venv"
LOG_DIR="/var/log/aml_pipeline"

# --- Prep ---
mkdir -p "$LOG_DIR"

echo "🟡 Starting scoring..."
cd "$PROJECT_DIR"
# Activate virtual environment
# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

# --- Helpers ---
die() { echo "❌ $*" >&2; deactivate || true; exit 1; }

is_yyyymmdd() {
  [[ "$1" =~ ^[0-9]{8}$ ]] || return 1
  # Validate actual date (requires GNU date)
  date -d "${1:0:4}-${1:4:2}-${1:6:2}" +%Y%m%d >/dev/null 2>&1
}

next_day() {
  date -d "${1:0:4}-${1:4:2}-${1:6:2} +1 day" +%Y%m%d
}

# --- Args ---
[[ $# -ge 1 ]] || die "Provide a date (YYYYMMDD) or a range (YYYYMMDD-YYYYMMDD)."

ARG="$1"

# Master rolling log
MASTER_LOG="$LOG_DIR/score.log"

run_one_day() {
  local d="$1"
  local day_log="$LOG_DIR/score_${d}.log"
  echo "▶️  $d : starting scoring (logs: $day_log)"
  # Run with low CPU priority; nohup to survive terminal close; wait for completion
  set +e
  nohup nice -n 1 python3 score.py "$d" >>"$day_log" 2>&1 &
  pid=$!
  wait "$pid"
  rc=$?
  set -e
  if [[ $rc -eq 0 ]]; then
    echo "✅ $d : finished successfully"
    echo "$(date -Is) DONE $d" >>"$MASTER_LOG"
  else
    echo "🛑 $d : FAILED with exit code $rc (see $day_log)"
    echo "$(date -Is) FAIL $d rc=$rc" >>"$MASTER_LOG"
    return $rc
  fi
}

# --- Main ---
if [[ "$ARG" == *"-"* ]]; then
  # Range mode
  START="${ARG%-*}"
  END="${ARG#*-}"
  is_yyyymmdd "$START" || die "Invalid start date: $START (expected YYYYMMDD)"
  is_yyyymmdd "$END"   || die "Invalid end date: $END (expected YYYYMMDD)"

  # Normalize order if user inverted
  if [[ "$START" > "$END" ]]; then
    tmp="$START"; START="$END"; END="$tmp"
  fi

  echo "📆 Running sequential scoring from $START to $END (inclusive)..."
  CUR="$START"
  while true; do
    run_one_day "$CUR"
    [[ "$CUR" == "$END" ]] && break
    CUR="$(next_day "$CUR")"
  done
  echo "🎉 Range completed: $START → $END. See per-day logs in $LOG_DIR and $MASTER_LOG."

else
  # Single day mode
  DATE="$ARG"
  is_yyyymmdd "$DATE" || die "Invalid date: $DATE (expected YYYYMMDD)"

  echo "📅 Running single-day scoring for $DATE..."
  run_one_day "$DATE"
  echo "🎉 Done. See $LOG_DIR/score_${DATE}.log and $MASTER_LOG."
fi

# Deactivate venv
deactivate
