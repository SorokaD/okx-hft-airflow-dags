"""
Common utilities for OKX DAGs.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def get_current_timestamp_ms() -> int:
    """Get current UTC timestamp in milliseconds."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def log_task_start(task_name: str) -> None:
    """Log task start with timestamp."""
    logger.info(f"[{task_name}] Started at {datetime.now(timezone.utc).isoformat()}")


def log_task_end(task_name: str) -> None:
    """Log task end with timestamp."""
    logger.info(f"[{task_name}] Finished at {datetime.now(timezone.utc).isoformat()}")


