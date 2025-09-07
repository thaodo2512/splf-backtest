#!/usr/bin/env bash
set -euo pipefail

# Full end-to-end pipeline runner.
# Usage:
#   bash scripts/run_e2e.sh [config_path]
# Example:
#   bash scripts/run_e2e.sh config/config.yaml
#
# Notes:
# - Run after activating your env: `conda activate splf`
# - All output is appended to debug.log

CONFIG_PATH="${1:-config/config.yaml}"
LOG_FILE="debug.log"

echo "=== E2E START $(date) config=${CONFIG_PATH}" | tee -a "$LOG_FILE"

# Load .env if present to export API keys (e.g., COINALYZE_API_KEY)
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
  echo "Loaded .env" | tee -a "$LOG_FILE"
fi

echo "[1/6] Checking environment…" | tee -a "$LOG_FILE"
python scripts/check_env.py | tee -a "$LOG_FILE" || true

echo "[2/7] Downloading data…" | tee -a "$LOG_FILE"
python scripts/download_data.py --config "$CONFIG_PATH" | tee -a "$LOG_FILE"

echo "[3/7] Ingesting Binance REST (funding/OI/liqs)…" | tee -a "$LOG_FILE"
python scripts/ingest_binance.py --config "$CONFIG_PATH" | tee -a "$LOG_FILE" || true

echo "[4/7] Building 1-minute bars…" | tee -a "$LOG_FILE"
python scripts/build_minute_bars.py --config "$CONFIG_PATH" | tee -a "$LOG_FILE"

echo "[5/7] Computing features…" | tee -a "$LOG_FILE"
python scripts/compute_features.py --config "$CONFIG_PATH" | tee -a "$LOG_FILE"

echo "[6/7] Running backtest…" | tee -a "$LOG_FILE"
python scripts/run_backtest.py --config "$CONFIG_PATH" | tee -a "$LOG_FILE"

echo "[7/7] Analyzing results…" | tee -a "$LOG_FILE"
python scripts/analyze_results.py --config "$CONFIG_PATH" | tee -a "$LOG_FILE"

# Optional visualization (uses first symbol in config if --symbol not provided)
if command -v python >/dev/null 2>&1; then
  echo "[opt] Visualizing minute bars…" | tee -a "$LOG_FILE"
  python scripts/visualize_minute_bar.py --config "$CONFIG_PATH" | tee -a "$LOG_FILE" || true
fi

echo "=== E2E DONE $(date)" | tee -a "$LOG_FILE"
