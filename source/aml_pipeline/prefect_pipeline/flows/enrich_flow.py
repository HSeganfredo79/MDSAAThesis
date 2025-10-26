from datetime import datetime, timedelta, timezone
from typing import Optional
from prefect import flow, task, get_run_logger
from flows.utils import run_script  # uses AML_PIPELINE_HOME to find enrich.py

def _yyyymmdd_utc(days_back: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y%m%d")

@task(retries=2, retry_delay_seconds=60)
def enrich_day(date_yyyymmdd: str):
    logger = get_run_logger()
    logger.info(f"Enrichment started for {date_yyyymmdd}")
    run_script("enrich.py", [date_yyyymmdd])
    logger.info(f"Enrichment finished for {date_yyyymmdd}")

@flow(name="Daily Enrichment")
def enrich_flow(date: Optional[str] = None):
    """
    Enrich a specific UTC day (YYYYMMDD).
    If no date is provided, defaults to D-1 at runtime.
    """
    if not date:
        date = _yyyymmdd_utc(1)  # D-1
    enrich_day(date)

if __name__ == "__main__":
    enrich_flow()
