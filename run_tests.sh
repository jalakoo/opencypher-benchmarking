#!/usr/bin/env bash
set -euo pipefail

VENV_DIR=".venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
pip install -q -e ".[dev]"

echo ""
echo "=== Ruff check ==="
ruff check src/ tests/

echo ""
echo "=== Ruff format ==="
ruff format --check src/ tests/

echo ""
echo "=== Pytest ==="
pytest tests/ -v

echo ""
echo "=== All checks passed ==="
