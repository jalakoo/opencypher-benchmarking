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

exec graph-db-bench "$@"
