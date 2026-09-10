"""Pytest path bootstrap: Airflow DAGs live under dags/."""

from __future__ import annotations

import sys
from pathlib import Path

_DAGS = Path(__file__).resolve().parent.parent / "dags"
if str(_DAGS) not in sys.path:
    sys.path.insert(0, str(_DAGS))
