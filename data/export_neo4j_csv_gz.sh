#!/usr/bin/env bash
# One-file CSV export samples for thesis demo:
#  - Rows: :Transaction in April 2025 (UTC)
#  - Columns: ALL tx properties + from_addr + to_addr
#  - Final Output: /var/backups/neo4j/exports/april2025-sample.csv.gz
# Strategy: export per-day CSV (.csv.gz), then stream-merge into one CSV.gz
# NOTE: Contains plaintext credentials at user request.

set -Eeuo pipefail

DB_NAME="${DB_NAME:-neo4j}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-PotatoDTND12!}"   # << inserted as requested
BACKUP_DIR="${BACKUP_DIR:-/var/backups/neo4j/exports}"
BATCH_SIZE="${BATCH_SIZE:-20000}"

# Window (UTC) — full month of April 2025 (change via env if needed)
START_DAY="${START_DAY:-2025-04-01}"   # inclusive
END_DAY="${END_DAY:-2025-05-01}"       # exclusive

# Discover Neo4j import dir (APOC writes here)
IMPORT_DIR="$(awk -F= '/^\s*server.directories.import\s*=/{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2}' /etc/neo4j/neo4j.conf || true)"
[ -z "${IMPORT_DIR}" ] && IMPORT_DIR="/var/lib/neo4j/import"

# Working subdir + final names (renamed to "sample")
SUBDIR_NAME="sample-$(date -u +%Y%m%dT%H%M%SZ)"
SUBDIR="${IMPORT_DIR}/${SUBDIR_NAME}"
FINAL_GZ="${BACKUP_DIR}/april2025-sample.csv.gz"

# Auth args
CYPHER_AUTH=(-u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}")

# Preflight
command -v cypher-shell >/dev/null || { echo "cypher-shell not found"; exit 1; }
sudo install -o neo4j -g neo4j -m 750 -d "${SUBDIR}"
sudo install -o root  -g root  -m 750 -d "${BACKUP_DIR}"

# Verify APOC
cypher-shell "${CYPHER_AUTH[@]}" -d "${DB_NAME}" "RETURN apoc.version()" >/dev/null || {
  echo "APOC not available. Check apoc.conf/neo4j.conf."; exit 1; }

echo "[*] Exporting April 2025 day-by-day to gzipped CSVs (sample)…"
current="${START_DAY}"
files=()

while [ "$(date -u -d "${current}" +%s)" -lt "$(date -u -d "${END_DAY}" +%s)" ]; do
  next="$(date -u -d "${current} +1 day" +%Y-%m-%d)"
  start_epoch="$(date -u -d "${current} 00:00:00Z" +%s)"
  end_epoch="$(date -u -d "${next} 00:00:00Z" +%s)"

  # Explicit .csv.gz per day to avoid name collisions and ensure compression
  out_base="april2025-sample-${current}.csv.gz"
  out_param="${SUBDIR_NAME}/${out_base}"          # passed to APOC
  out_path="${SUBDIR}/${out_base}"                # expected path on disk
  alt_plain="${SUBDIR}/april2025-sample-${current}.csv"  # fallback if APOC wrote plain .csv

  echo "  - ${current}"
  cypher-shell "${CYPHER_AUTH[@]}" -d "${DB_NAME}" \
    --param "start   => ${start_epoch}" \
    --param "end     => ${end_epoch}" \
    --param "outfile => '${out_param}'" \
    "CALL apoc.export.csv.query(
       'MATCH (src:Wallet)-[:SENT]->(tx:Transaction)-[:RECEIVED_BY]->(dst:Wallet)
        WHERE tx.timestamp >= \$start AND tx.timestamp < \$end
        // ALL tx properties + wallet addresses (no explicit prop list)
        RETURN tx, src.address AS from_addr, dst.address AS to_addr',
       \$outfile,
       {quotes:'ifNeeded',
        compression:'GZIP',
        batchSize:${BATCH_SIZE},
        params:{start:\$start, end:\$end}}
     )
     YIELD file, rows
     RETURN file, rows" >/dev/null

  # Only add if APOC actually produced output. Support both .csv.gz and .csv.
  if sudo test -f "${out_path}"; then
    files+=("${out_path}")
  elif sudo test -f "${alt_plain}"; then
    files+=("${alt_plain}")
  fi

  current="${next}"
done

if [ ${#files[@]} -eq 0 ]; then
  echo "No daily files produced. Nothing to merge."; exit 1
fi

# Sort files to ensure chronological order
IFS=$'\n' files=($(printf '%s\n' "${files[@]}" | sort)) ; unset IFS

echo "[*] Merging ${#files[@]} daily files into one CSV.gz (preserve only first header)…"
# helper to cat first line (header) and body across gz or plain files
emit_header() {
  local f="$1"
  if [[ "$f" == *.gz ]]; then
    gzip -dc "$f" | head -n 1
  else
    head -n 1 "$f"
  fi
}
emit_body() {
  local f="$1"
  if [[ "$f" == *.gz ]]; then
    gzip -dc "$f" | tail -n +2
  else
    tail -n +2 "$f"
  fi
}

{
  emit_header "${files[0]}"
  for f in "${files[@]}"; do
    emit_body "$f"
  done
} | gzip > "${FINAL_GZ}"

echo "[*] Cleaning working folder…"
sudo rm -rf "${SUBDIR}"

echo "✅ Done.
CSV (gz): ${FINAL_GZ}

Preview without unpacking:
  zcat ${FINAL_GZ} | head -n 5
  zcat ${FINAL_GZ} | wc -l

If needed (heap), lower batch size:
  BATCH_SIZE=5000 sudo ./export_neo4j_csv_gz.sh
"
