"""
Test that all DAGs can be imported without errors.

This test recursively imports all .py files from dags/ directory
and fails if any DAG has import errors or syntax issues.
"""

import importlib.util
import sys
from pathlib import Path

import pytest


def get_dag_files() -> list[Path]:
    """Get all Python files from dags/ directory."""
    dags_dir = Path(__file__).parent.parent / "dags"
    return list(dags_dir.rglob("*.py"))


def import_module_from_path(path: Path) -> None:
    """Import a Python module from file path."""
    module_name = path.stem
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load spec for {path}")
    
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)


class TestDagImports:
    """Test suite for DAG imports."""

    def test_all_dag_files_exist(self) -> None:
        """Verify that DAG files exist."""
        dag_files = get_dag_files()
        assert len(dag_files) > 0, "No DAG files found in dags/ directory"

    @pytest.mark.parametrize("dag_file", get_dag_files(), ids=lambda p: str(p.name))
    def test_dag_imports(self, dag_file: Path) -> None:
        """Test that each DAG file can be imported without errors."""
        # Skip __init__.py files
        if dag_file.name == "__init__.py":
            pytest.skip("Skipping __init__.py")
        
        # Add dags directory to path for imports to work
        dags_dir = Path(__file__).parent.parent / "dags"
        if str(dags_dir) not in sys.path:
            sys.path.insert(0, str(dags_dir))
        
        try:
            import_module_from_path(dag_file)
        except Exception as e:
            pytest.fail(f"Failed to import {dag_file}: {e}")

    def test_health_check_dag_exists(self) -> None:
        """Verify that the health check DAG exists and can be imported."""
        dags_dir = Path(__file__).parent.parent / "dags"
        if str(dags_dir) not in sys.path:
            sys.path.insert(0, str(dags_dir))
        
        health_check_path = dags_dir / "okx" / "pipelines" / "okx_health_checks.py"
        assert health_check_path.exists(), "Health check DAG file not found"
        
        import_module_from_path(health_check_path)


