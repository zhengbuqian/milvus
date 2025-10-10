#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
UI_DIR="${SCRIPT_DIR}"

# Args: [vite_dev_port] [bind_host] [data_port]
DEV_PORT=${1:-5173}
BIND=${2:-127.0.0.1}
DATA_PORT=${3:-4173}

is_port_in_use() {
  local port=$1
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP -sTCP:LISTEN -P -n | grep -q ":${port}"
  elif command -v ss >/dev/null 2>&1; then
    ss -ltn | awk '{print $4}' | grep -q ":${port}$"
  elif command -v netstat >/dev/null 2>&1; then
    netstat -ltn | awk '{print $4}' | grep -q ":${port}$"
  else
    return 1
  fi
}

find_free_port() {
  local start=$1
  local attempts=0
  local port=$start
  while is_port_in_use "$port"; do
    attempts=$((attempts+1))
    port=$((port+1))
    [ $attempts -ge 50 ] && { echo "No free port found starting at ${start}" >&2; exit 1; }
  done
  echo "$port"
}

echo "Starting Scalar Bench UI (dev mode with HMR)"
echo "--------------------------------------------"
echo "Root directory : ${PROJECT_ROOT}"
echo "Dev server URL : http://${BIND}:${DEV_PORT}/ (will adjust if busy)"
echo "Results (proxy): http://${BIND}:${DATA_PORT}/_artifacts/"
echo

# Ensure deps
cd "${UI_DIR}"
echo "Installing dependencies (if needed)..."
npm install --no-audit --no-fund >/dev/null 2>&1 || npm install

# Start python static server for results (background). Ignore error if port is busy.
cd "${PROJECT_ROOT}"
PY_PID=""
if is_port_in_use "${DATA_PORT}"; then
  echo "Reusing existing results server on :${DATA_PORT}"
else
  echo "Starting results server on :${DATA_PORT}"
  python3 -m http.server "${DATA_PORT}" --bind "${BIND}" >/dev/null 2>&1 &
  PY_PID=$!
  trap 'if [ -n "${PY_PID}" ] && kill -0 ${PY_PID} >/dev/null 2>&1; then echo "Stopping results server (${PY_PID})"; kill ${PY_PID} >/dev/null 2>&1 || true; fi' EXIT INT TERM
fi

# Start Vite dev server (foreground)
cd "${UI_DIR}"
if is_port_in_use "${DEV_PORT}"; then
  NEW_DEV_PORT=$(find_free_port "${DEV_PORT}")
  if [ "${NEW_DEV_PORT}" != "${DEV_PORT}" ]; then
    echo "Port ${DEV_PORT} in use; switching to ${NEW_DEV_PORT}"
    DEV_PORT="${NEW_DEV_PORT}"
  fi
fi
echo "Starting Vite dev server with HMR on :${DEV_PORT}..."
exec npm run dev -- --strictPort --port "${DEV_PORT}" --host "${BIND}"


