#!/usr/bin/env bash
set -euo pipefail

VENV_DIR=".venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

# Install with all extras (server + embedded DBs)
# Check for a driver package to detect if extras are actually installed
if ! python -c "import neo4j" &>/dev/null; then
    echo "Installing graph-db-comparison[all]..."
    pip install -q -e ".[all]"
fi

START_TIME=$SECONDS
graph-db-bench "$@"
ELAPSED=$(( SECONDS - START_TIME ))
echo "Total benchmark time: $((ELAPSED / 60))m $((ELAPSED % 60))s"

# Auto-open the generated report if it exists
REPORT="./reports/report.html"
if [ -f "$REPORT" ]; then
    echo "Opening $REPORT..."
    open "$REPORT"
fi
