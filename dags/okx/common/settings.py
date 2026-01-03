"""
Common settings for OKX Airflow DAGs.

No secrets here - all connections and variables are configured
via Airflow UI or environment variables.
"""

from datetime import timedelta

# Timezone
DEFAULT_TZ = "UTC"

# Owner for DAGs
OWNER = "okx-data"

# Default arguments for all DAGs
DEFAULT_ARGS = {
    "owner": OWNER,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Common tags
TAGS_OKX = ["okx"]
TAGS_DATA = ["okx", "data"]
TAGS_HEALTH = ["okx", "health"]


