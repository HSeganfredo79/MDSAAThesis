#!/usr/bin/env bash
set -euo pipefail
echo "== PATH =="
echo "$PATH"
echo "== Where is prefect? =="
command -v prefect || true
echo "== Prefect version =="
prefect version || true
echo "== Processes =="
for p in 846 3031; do
  [ -d "/proc/$p" ] && { echo "-- PID $p"; tr '\0' ' ' </proc/$p/cmdline; echo; }
done
echo "== Services =="
systemctl is-enabled prefect-server prefect-worker || true
systemctl status prefect-server --no-pager || true
systemctl status prefect-worker --no-pager || true
echo "== Unit files =="
systemctl cat prefect-server || true
systemctl cat prefect-worker || true
echo "== Worker & pools =="
prefect work-pool ls || true
prefect worker ls || true
