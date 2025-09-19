#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
UI_DIR="${SCRIPT_DIR}"
PORT=${1:-4173}
BIND=${2:-127.0.0.1}

cd "${UI_DIR}"

echo "Installing dependencies and building UI..."
npm install --no-audit --no-fund >/dev/null 2>&1 || npm install
npm run build

cd "${PROJECT_ROOT}"

cat <<MSG
Serving Scalar Bench UI
-----------------------
Root directory : ${PROJECT_ROOT}
UI entry point : http://${BIND}:${PORT}/ui/dist/index.html
Results path   : ${PROJECT_ROOT}/_artifacts/results/

Override the results base path from the UI badge if you have copied artifacts elsewhere.
Use Ctrl+C to stop the server.
MSG

python3 -m http.server "${PORT}" --bind "${BIND}"