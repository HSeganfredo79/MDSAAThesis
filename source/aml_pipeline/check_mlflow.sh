#!/usr/bin/env bash
# Resolve MLflow run_id -> (model_name, version, stage) using the MLflow REST API.
# Requires: curl, jq
#
# Usage:
#   ./mlflow_lookup.sh --uri http://localhost:5000 RUN_ID [RUN_ID ...]
#   MLFLOW_TRACKING_URI=http://localhost:5000 ./mlflow_lookup.sh RUN_ID [RUN_ID ...]
#
# Notes:
# - Tries Model Registry first; falls back to run tags:
#     registered_model_name / registered_model_version / registered_model_stage

set -euo pipefail

URI=""
if [[ "${1:-}" == "--uri" ]]; then
  shift
  URI="${1:-}"
  shift || true
fi
URI="${URI:-${MLFLOW_TRACKING_URI:-}}"

if [[ -z "${URI}" ]]; then
  echo "Error: MLflow tracking URI not provided. Use --uri or set MLFLOW_TRACKING_URI." >&2
  exit 1
fi
command -v curl >/dev/null || { echo "Error: curl is required." >&2; exit 1; }
command -v jq   >/dev/null || { echo "Error: jq is required."   >&2; exit 1; }

if [[ "$#" -lt 1 ]]; then
  echo "Usage: $0 [--uri http://host:5000] RUN_ID [RUN_ID ...]" >&2
  exit 1
fi

echo "Tracking URI: ${URI}"
printf "%-36s  %-30s  %-8s  %-12s  %-8s\n" "run_id" "model_name" "version" "stage" "source"
printf "%0.s-" {1..100}; echo

lookup_registry() {
  local run_id="$1"
  # POST /api/2.0/mlflow/model-versions/search  body: {"filter":"run_id='RID'"}
  local resp
  if ! resp="$(curl -sfS -X POST "${URI%/}/api/2.0/mlflow/model-versions/search" \
    -H "Content-Type: application/json" \
    -d "{\"filter\":\"run_id='${run_id}'\"}")"; then
    return 1
  fi
  local count
  count="$(jq '.model_versions | length' <<<"$resp" 2>/dev/null || echo 0)"
  if [[ "$count" -gt 0 ]]; then
    local name ver stage
    name="$(jq -r '.model_versions[0].name // "n/a"' <<<"$resp")"
    ver="$(jq -r '.model_versions[0].version // "n/a"' <<<"$resp")"
    stage="$(jq -r '.model_versions[0].current_stage // "n/a"' <<<"$resp")"
    printf "%s\t%s\t%s\n" "$name" "$ver" "$stage"
    return 0
  fi
  return 1
}

lookup_tags() {
  local run_id="$1"
  # GET /api/2.0/mlflow/runs/get?run_id=RID
  local resp
  if ! resp="$(curl -sfS "${URI%/}/api/2.0/mlflow/runs/get?run_id=${run_id}")"; then
    return 1
  fi
  # tags are an array of objects: [{"key":"...","value":"..."}, ...]
  local name ver stage
  name="$(jq -r '.run.data.tags[]? | select(.key=="registered_model_name")   | .value' <<<"$resp" | head -n1)"
  ver="$(  jq -r '.run.data.tags[]? | select(.key=="registered_model_version")| .value' <<<"$resp" | head -n1)"
  stage="$(jq -r '.run.data.tags[]? | select(.key=="registered_model_stage") | .value' <<<"$resp" | head -n1)"
  [[ -z "${name}"  ]] && name="n/a"
  [[ -z "${ver}"   ]] && ver="n/a"
  [[ -z "${stage}" ]] && stage="n/a"
  printf "%s\t%s\t%s\n" "$name" "$ver" "$stage"
  return 0
}

for rid in "$@"; do
  name="n/a"; ver="n/a"; stage="n/a"; src="none"
  if reg_out="$(lookup_registry "$rid" 2>/dev/null)"; then
    name="$(cut -f1 <<<"$reg_out")"
    ver="$(cut -f2 <<<"$reg_out")"
    stage="$(cut -f3 <<<"$reg_out")"
    src="registry"
  elif tag_out="$(lookup_tags "$rid" 2>/dev/null)"; then
    name="$(cut -f1 <<<"$tag_out")"
    ver="$(cut -f2 <<<"$tag_out")"
    stage="$(cut -f3 <<<"$tag_out")"
    src="tags"
  fi
  printf "%-36s  %-30s  %-8s  %-12s  %-8s\n" "$rid" "$name" "$ver" "$stage" "$src"
done
