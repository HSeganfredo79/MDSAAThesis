from prefect import flow, task, get_run_logger
import os, sys, subprocess
from pathlib import Path

PIPELINE_HOME = Path(os.environ.get("AML_PIPELINE_HOME", "/root/tese_henrique/aml_pipeline"))
LOG_DIR = Path("/var/log/aml_pipeline")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "kafka_consumer.log"

@task
def start_consumer() -> int:
    logger = get_run_logger()
    script = PIPELINE_HOME / "kafka_consumer.py"
    if not script.exists():
        raise FileNotFoundError(str(script))
    logger.info("🔄 Starting Kafka Consumer: %s %s", sys.executable, script)
    with open(LOG_FILE, "ab", buffering=0) as lf:
        proc = subprocess.Popen([sys.executable, str(script)],
                                cwd=str(PIPELINE_HOME),
                                stdout=lf, stderr=lf)
    logger.info("✅ Consumer PID=%s; logging to %s", proc.pid, LOG_FILE)
    return proc.pid

@flow(name="Kafka Consumer Flow")
def consumer_flow():
    start_consumer()
