#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v node >/dev/null 2>&1; then
  echo "[serve_ui] Node.js is required. Please install Node >= 18." >&2
  exit 1
fi

PKG_MANAGER=""
if command -v pnpm >/dev/null 2>&1; then
  PKG_MANAGER="pnpm"
elif command -v yarn >/dev/null 2>&1; then
  PKG_MANAGER="yarn"
else
  PKG_MANAGER="npm"
fi

echo "[serve_ui] Using package manager: ${PKG_MANAGER}"

# Install deps if needed
if [ ! -d node_modules ]; then
  echo "[serve_ui] Installing dependencies..."
  if [ "${PKG_MANAGER}" = "pnpm" ]; then pnpm install --frozen-lockfile || pnpm install; fi
  if [ "${PKG_MANAGER}" = "yarn" ]; then yarn install --frozen-lockfile || yarn install; fi
  if [ "${PKG_MANAGER}" = "npm" ]; then npm ci || npm install; fi
fi

echo "[serve_ui] Building UI..."
if [ "${PKG_MANAGER}" = "pnpm" ]; then pnpm run build; fi
if [ "${PKG_MANAGER}" = "yarn" ]; then yarn build; fi
if [ "${PKG_MANAGER}" = "npm" ]; then npm run build --silent; fi

echo "[serve_ui] Build complete. Output: dist/"
echo "[serve_ui] To preview locally on http://localhost:5173, run:"
echo "[serve_ui]   ${PKG_MANAGER} run preview"

