#!/bin/bash
set -euo pipefail

echo "Starting lab6 pipeline container"

# run the main pipeline (this script should return after work is done)
python src/main.py || true

echo "Pipeline finished — container will stay alive for inspection."

# Keep container alive
tail -f /dev/null
