#!/usr/bin/env bash
# check_running.sh — guard against starting ingestion when Kafka producer/consumer are running.
# Usage:
#   ./check_running.sh             # check only
#   ./check_running.sh --execute   # run ./run.sh if safe
#   ./check_running.sh --force     # ignore guards and run ./run.sh

set -Eeuo pipefail

VENV_BIN="/root/tese_henrique/aml_pipeline/.venv/bin"
RUN_SH="./run.sh"

DO_EXECUTE=0
DO_FORCE=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --execute) DO_EXECUTE=1 ;;
    --force)   DO_FORCE=1   ;;
    --check-only) DO_EXECUTE=0; DO_FORCE=0 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
  shift
done

export PATH="${VENV_BIN}:$PATH"

if ! command -v prefect >/dev/null 2>&1; then
  echo "[ERR] Prefect CLI not found in ${VENV_BIN}" >&2
  exit 2
fi

# Prefer env; else default; verify with /api/health. If env fails, try default.
API="${PREFECT_API_URL:-}"
if [[ -n "$API" ]] && curl -fsS --max-time 2 "${API%/}/health" >/dev/null; then
  PREFECT_API_URL="${API%/}"
elif curl -fsS --max-time 2 "http://127.0.0.1:4200/api/health" >/dev/null; then
  PREFECT_API_URL="http://127.0.0.1:4200/api"
else
  echo "[ERR] Cannot reach Prefect API via env or http://127.0.0.1:4200/api" >&2
  exit 2
fi
export PREFECT_API_URL
echo "[OK] Using PREFECT_API_URL=$PREFECT_API_URL"

# Get running/pending runs as JSON
prefect_runs_json() {
  prefect flow-run ls --state Running,Pending --limit 500 --json 2>/dev/null || echo '[]'
}

echo "[*] Checking Prefect for Kafka runs…"
ACTIVE_JSON="$(prefect_runs_json || echo '[]')"
ACTIVE_COUNT="$(
python3 - <<'PY' <<<"$ACTIVE_JSON"
import json, re, sys
data = json.load(sys.stdin)
pat = re.compile(r'kafka', re.I)
print(sum(1 for r in data if pat.search((r.get('deployment_name') or '') + (r.get('flow_name') or '') + (r.get('name') or ''))))
PY
)"
if [[ "$ACTIVE_COUNT" -gt 0 ]]; then
  echo "[COLLISION] ${ACTIVE_COUNT} Kafka-related Prefect runs:"
  python3 - <<'PY' <<<"$ACTIVE_JSON"
import json, re, sys
pat = re.compile(r'kafka', re.I)
rows = []
for r in json.load(sys.stdin):
    fn = r.get('flow_name',''); dn = r.get('deployment_name',''); n = r.get('name',''); st = r.get('state','')
    if pat.search(fn) or pat.search(dn) or pat.search(n):
        rows.append((fn,dn,n,st))
hdr = ("FLOW","DEPLOYMENT","RUN NAME","STATE")
w = [max(len(x[i]) for x in rows+[hdr]) for i in range(4)]
fmt = "  %-{}s | %-{}s | %-{}s | %-{}s".format(*w)
print(fmt % hdr); print("-"*(sum(w)+9))
for r in rows: print(fmt % r)
PY
  if [[ "$DO_FORCE" -ne 1 ]]; then
    echo "[ABORT] Refusing to proceed. Use --force to ignore."
    exit 3
  else
    echo "[WARN] FORCE enabled: continuing despite active Prefect runs."
  fi
else
  echo "[OK] No active Kafka-related Prefect runs."
fi

echo "[*] Checking OS processes for kafka_producer.py / kafka_consumer.py / kafka_streaming.py…"
if pgrep -af 'kafka_producer.py|kafka_consumer.py|kafka_streaming.py' >/dev/null; then
  echo "[COLLISION] Kafka processes running:"
  pgrep -af 'kafka_producer.py|kafka_consumer.py|kafka_streaming.py' | sed 's/^/  /'
  if [[ "$DO_FORCE" -ne 1 ]]; then
    echo "[ABORT] Refusing to proceed. Use --force to ignore."
    exit 3
  else
    echo "[WARN] FORCE enabled: continuing despite OS processes."
  fi
else
  echo "[OK] No Kafka OS processes found."
fi

echo "[SAFE] You can run ./run.sh now."

if [[ "$DO_EXECUTE" -eq 1 || "$DO_FORCE" -eq 1 ]]; then
  if [[ ! -x "$RUN_SH" ]]; then
    echo "[ERR] $RUN_SH not found or not executable."
    exit 4
  fi
  echo "[RUN] Executing $RUN_SH …"
  "$RUN_SH"
  rc=$?
  echo "[DONE] $RUN_SH exited with code $rc"
  exit $rc
fi

exit 0
