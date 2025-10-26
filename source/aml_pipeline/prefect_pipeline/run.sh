#!/usr/bin/env bash
# Manual runs for Prefect deployments (detached from deploy.sh)
# Uses flow defaults (D-1 for enrich, D-2 for score). You can still override via CLI args.

set -Eeuo pipefail

# Ensure Prefect CLI is available when run from cron/systemd
export PATH="/root/tese_henrique/aml_pipeline/.venv/bin:$PATH"
# Talk to the local Prefect server (matches your worker unit)
export PREFECT_API_URL="http://127.0.0.1:4200/api"

LOG_DIR=/var/log/aml_pipeline
mkdir -p "$LOG_DIR"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

log() { echo "[$(ts)] $*" | tee -a "$LOG_DIR/run.sh.log"; }

run_deploy() {
  local fqdn="$1"; shift || true
  local params=("$@")
  if [ ${#params[@]} -gt 0 ]; then
    log "Triggering: $fqdn with params: ${params[*]}"
    prefect deployment run "$fqdn" "${params[@]}" >> "$LOG_DIR/prefect-$(echo "$fqdn" | tr ' /' '__').log" 2>&1
  else
    log "Triggering: $fqdn"
    prefect deployment run "$fqdn" >> "$LOG_DIR/prefect-$(echo "$fqdn" | tr ' /' '__').log" 2>&1
  fi
}

# ── Optional explicit dates (uncomment to force) ───────────────
# ENRICH_DATE=$(date -u -d "yesterday" +%Y%m%d)
# SCORE_DATE=$(date -u -d "2 days ago" +%Y%m%d)

# If you uncomment the two lines above, also change the two calls below
# to add:  --params "{\"date\":\"$ENRICH_DATE\"}"  and  --params "{\"date\":\"$SCORE_DATE\"}"

# Enrich (defaults to D-1)
run_deploy "Daily Enrichment/enrich-daily"

# Score (defaults to D-2)
run_deploy "Daily Scoring/score-daily"

# Weekly training (defaults to days_ago=30; override like: --params '{"days_ago":14}')
run_deploy "Weekly Training/train-weekly"

# Drift monitor (no params)
run_deploy "Drift Monitor/drift-monitor"

# Kafka pieces are manual; leave unscheduled and trigger when you want.
# NOTE: our deployment names are "Kafka Producer" and "Kafka Consumer" (no "Flow")
# run_deploy "Kafka Producer/kafka-producer"
# run_deploy "Kafka Consumer/kafka-consumer"

# If you want the combined loop:
run_deploy "Kafka Streaming Loop/kafka-loop"

log "All triggers submitted."
