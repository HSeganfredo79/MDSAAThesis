from datetime import datetime, timedelta, timezone
from typing import Optional
from prefect import flow, task, get_run_logger
from flows.utils import run_script  # uses AML_PIPELINE_HOME to find score.py

def _yyyymmdd_utc(days_back: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y%m%d")

@task(retries=2, retry_delay_seconds=60)
def score_day(date_yyyymmdd: str):
    logger = get_run_logger()
    logger.info(f"Scoring started for {date_yyyymmdd}")
    run_script("score.py", [date_yyyymmdd])
    logger.info(f"Scoring finished for {date_yyyymmdd}")

@flow(name="Daily Scoring")
def score_flow(date: Optional[str] = None):
    """
    Score a specific UTC day (YYYYMMDD).
    If no date is provided, defaults to D-2 at runtime
    (to ensure enrichment is complete).
    """
    if not date:
        date = _yyyymmdd_utc(2)  # D-2
    score_day(date)

if __name__ == "__main__":
    score_flow()
