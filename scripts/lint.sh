#!/bin/bash
# Lint script for OKX Airflow DAGs
set -e

echo "Running ruff..."
ruff check dags/ tests/

echo "Running black --check..."
black --check dags/ tests/

echo "✅ All lint checks passed!"


