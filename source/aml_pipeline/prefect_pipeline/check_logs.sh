#!/usr/bin/env bash
# check_logs.sh — tail selected AML pipeline logs (robust)
# Usage:
#   chmod +x check_logs.sh
#   ./check_logs.sh                  # last 200 lines each
#   TAIL_LINES=500 ./check_logs.sh   # change tail length

set -u -o pipefail   # no `-e` so one failure won't abort the script

LOG_DIR="/var/log/aml_pipeline"
TAIL_LINES="${TAIL_LINES:-200}"

hr(){ printf '\n==== %s (%s UTC) ====\n' "$1" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"; }

tail_if_exists() {
  local label="$1" file="$2"
  if [[ -f "$file" ]]; then
    echo "-- ${label}: ${file} (last ${TAIL_LINES} lines) --"
    tail -n "${TAIL_LINES}" "$file" || true
  else
    echo "-- ${label}: ${file} [missing] --"
  fi
}

tail_match() {
  local label="$1" pattern="$2"
  local matched=0
  shopt -s nullglob
  # capture matches explicitly to avoid word-splitting woes
  local files=( $pattern )
  shopt -u nullglob

  if ((${#files[@]})); then
    for f in "${files[@]}"; do
      if [[ -f "$f" ]]; then
        matched=1
        echo "-- ${label}: ${f} (last ${TAIL_LINES} lines) --"
        tail -n "${TAIL_LINES}" "$f" || true
      fi
    done
  fi

  if [[ $matched -eq 0 ]]; then
    echo "-- ${label}: ${pattern} [no matches] --"
  fi
}

# ---------------- sections ----------------

hr "RUN CONTROLLER LOG"
tail_if_exists "run.sh" "${LOG_DIR}/run.sh.log"

hr "PER-DEPLOYMENT TRIGGER LOGS"
# Support both single-underscore and double-underscore patterns
tail_match "Enrichment"        "${LOG_DIR}/prefect-*Enrichment*enrich-daily*.log"
tail_match "Scoring"           "${LOG_DIR}/prefect-*Scoring*score-daily*.log"
tail_match "Training"          "${LOG_DIR}/prefect-*Training*train-weekly*.log"
tail_match "Drift"             "${LOG_DIR}/prefect-*Drift*drift-monitor*.log"
tail_match "Kafka Loop"        "${LOG_DIR}/prefect-*Kafka*Loop*kafka-loop*.log"
tail_match "Kafka Producer"    "${LOG_DIR}/prefect-*Kafka*producer*.log"
tail_match "Kafka Consumer"    "${LOG_DIR}/prefect-*Kafka*consumer*.log"

hr "BUSINESS SCRIPT LOGS"
tail_if_exists "enrich.py"     "${LOG_DIR}/enrich.log"
tail_if_exists "score.py"      "${LOG_DIR}/score.log"
tail_if_exists "train.py"      "${LOG_DIR}/train.log"
tail_if_exists "monitor.py"    "${LOG_DIR}/monitor.log"

hr "KAFKA LOGS (if any)"
tail_if_exists "kafka_producer"   "${LOG_DIR}/kafka_producer.log"
tail_if_exists "kafka_consumer"   "${LOG_DIR}/kafka_consumer.log"
tail_if_exists "prefect-kafka-loop" "${LOG_DIR}/prefect-kafka-loop.log"
