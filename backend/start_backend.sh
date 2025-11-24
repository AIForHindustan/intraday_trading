#!/bin/bash
# Backend startup script that ensures venv Python is used

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR"
DASHBOARD_DIR="$(cd "$BACKEND_DIR/.." && pwd)"

# Activate venv
VENV_PATH="/Users/lokeshgupta/Desktop/aion_algo_trading/intraday_trading/.venv"
if [ ! -d "$VENV_PATH" ]; then
    echo "❌ ERROR: .venv not found at $VENV_PATH"
    exit 1
fi

# Activate venv
source "$VENV_PATH/bin/activate"

# Unset PYTHONPATH - venv should handle all package resolution
# The global PYTHONPATH in ~/.zshrc interferes with venv package resolution
unset PYTHONPATH

# Verify we're using venv Python
PYTHON_EXE=$(which python)
if [[ "$PYTHON_EXE" != *".venv"* ]]; then
    echo "❌ ERROR: Not using venv Python: $PYTHON_EXE"
    exit 1
fi

echo "✅ Using Python: $PYTHON_EXE"

# Set crawler and token lookup paths
export CRAWLER_CONFIG_PATH="/Users/lokeshgupta/Desktop/aion_algo_trading/zerodha_websocket/crawlers/binary_crawler1/binary_crawler1.json"
export TOKEN_LOOKUP_PATH="/Users/lokeshgupta/Desktop/aion_algo_trading/intraday_trading/core/data/token_lookup_enriched.json"

echo "✅ CRAWLER_CONFIG_PATH: $CRAWLER_CONFIG_PATH"
echo "✅ TOKEN_LOOKUP_PATH: $TOKEN_LOOKUP_PATH"
echo "✅ Starting backend on port 8000..."

# Set PYTHON environment variable so uvicorn reloader uses the correct Python
export PYTHON="$PYTHON_EXE"

# Start uvicorn with the venv Python (no --reload to avoid subprocess issues)
cd "$BACKEND_DIR"
exec "$PYTHON_EXE" -m uvicorn optimized_main:app --host 0.0.0.0 --port 8000

