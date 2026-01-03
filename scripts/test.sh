#!/bin/bash
# Test script for OKX Airflow DAGs
set -e

echo "Running pytest..."
pytest tests/ -v

echo "✅ All tests passed!"


