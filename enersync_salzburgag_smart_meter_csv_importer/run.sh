#!/usr/bin/env bash
set -euo pipefail

echo "[run.sh] reading options.json..."
cat /data/options.json || echo "[run.sh] options.json not found?"

INPUT_DIR=$(jq -r '.input_dir' /data/options.json)
PROCESSED_DIR=$(jq -r '.processed_dir' /data/options.json)
ERROR_DIR=$(jq -r '.error_dir' /data/options.json)

echo "[run.sh] ensuring folders:"
echo "  INPUT_DIR=$INPUT_DIR"
echo "  PROCESSED_DIR=$PROCESSED_DIR"
echo "  ERROR_DIR=$ERROR_DIR"
mkdir -p "$INPUT_DIR" "$PROCESSED_DIR" "$ERROR_DIR"

echo "[run.sh] Python:"
which python3 || true
python3 --version || true

echo "[run.sh] starting app..."
exec python3 /app/app.py
