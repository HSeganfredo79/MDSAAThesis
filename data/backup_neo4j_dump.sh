#!/usr/bin/env bash
# Offline backup (dump + gzip) for Neo4j 5.x Community on RHEL
# Creates /var/backups/neo4j/neo4j-YYYYMMDDTHHMMSSZ/neo4j.dump.gz
# Keeps the last 7 backups (rotate older ones)

set -Eeuo pipefail

DB_NAME="${DB_NAME:-neo4j}"
BACKUP_ROOT="${BACKUP_ROOT:-/var/backups/neo4j}"
RETAIN="${RETAIN:-7}"

DATE="$(date -u +%Y%m%dT%H%M%SZ)"
TMP_DIR="${BACKUP_ROOT}/.tmp-${DATE}"
OUT_DIR="${BACKUP_ROOT}/${DB_NAME}-${DATE}"
DUMPNAME="${DB_NAME}.dump"
DUMP_PATH="${TMP_DIR}/${DUMPNAME}"
DB_DIR="/var/lib/neo4j/data/databases/${DB_NAME}"

install -o neo4j -g neo4j -m 750 -d "${BACKUP_ROOT}"
install -o neo4j -g neo4j -m 750 -d "${TMP_DIR}"
#mkdir -p "${TMP_DIR}" "${BACKUP_ROOT}"

# (Optional) sanity check: enough space? (needs ~ DB size + margin)
DB_SIZE=$(du -sb "${DB_DIR}" | awk '{print $1}')
FREE=$(df -B1 "${BACKUP_ROOT}" | awk 'NR==2{print $4}')
if (( FREE < DB_SIZE * 2 )); then
  echo "ERROR: Not enough free space in ${BACKUP_ROOT}. Need ~$((DB_SIZE*2)) bytes, have ${FREE}." >&2
  exit 1
fi

echo "[1/5] Stopping Neo4j…"
systemctl stop neo4j

# Wait until JVM exits
for i in {1..60}; do
  if ! pgrep -u neo4j -f org.neo4j.server >/dev/null; then break; fi
  sleep 1
done
if pgrep -u neo4j -f org.neo4j.server >/dev/null; then
  echo "ERROR: Neo4j did not stop cleanly."; exit 1
fi

echo "[2/5] Dumping database '${DB_NAME}' to ${DUMP_PATH}…"
sudo -u neo4j /usr/bin/neo4j-admin database dump "${DB_NAME}" \
  --to-path="${TMP_DIR}" --overwrite-destination=true --verbose

echo "[3/5] Compressing dump…"
if command -v pigz >/dev/null 2>&1; then pigz -9 "${DUMP_PATH}"; else gzip -9 "${DUMP_PATH}"; fi

mkdir -p "${OUT_DIR}"
mv "${TMP_DIR}/${DUMPNAME}.gz" "${OUT_DIR}/"

echo "[4/5] Generating checksum…"
sha256sum "${OUT_DIR}/${DUMPNAME}.gz" > "${OUT_DIR}/${DUMPNAME}.gz.sha256"

echo "[5/5] Starting Neo4j…"
systemctl start neo4j

echo "Rotating old backups (keep ${RETAIN})…"
ls -1dt ${BACKUP_ROOT}/${DB_NAME}-* 2>/dev/null | tail -n +$((RETAIN+1)) | xargs -r rm -rf

echo "✅ Backup ready: ${OUT_DIR}/${DUMPNAME}.gz"
