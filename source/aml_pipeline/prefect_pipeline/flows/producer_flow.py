from prefect import flow, task, get_run_logger
import os, sys, shutil, subprocess
from pathlib import Path

# ---- Config via env with sensible defaults ----
PIPELINE_HOME = Path(os.environ.get("AML_PIPELINE_HOME", "/root/tese_henrique/aml_pipeline"))
KAFKA_HOME = Path(os.environ.get("KAFKA_HOME", "/opt/kafka"))
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "192.168.0.4:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "usdc-transactions")
PARTITIONS = int(os.environ.get("KAFKA_PARTITIONS", "8"))
REPLICATION = int(os.environ.get("KAFKA_REPLICATION", "1"))

LOG_DIR = Path("/var/log/aml_pipeline")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "kafka_producer.log"
KAFKA_TOPICS = KAFKA_HOME / "bin" / "kafka-topics.sh"

def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    logger = get_run_logger()
    logger.info("CMD: %s", " ".join(cmd))
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.stdout:
        logger.info(proc.stdout.rstrip())
    if check and proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc

@task
def ensure_kafka_service():
    if shutil.which("systemctl"):
        # start if not active
        if subprocess.run(["systemctl", "is-active", "--quiet", "kafka"]).returncode != 0:
            run_cmd(["systemctl", "start", "kafka"])
    else:
        get_run_logger().warning("systemctl not found; skipping kafka service control")

@task
def ensure_topic():
    if not KAFKA_TOPICS.exists():
        raise FileNotFoundError(f"{KAFKA_TOPICS} not found; set KAFKA_HOME")
    out = run_cmd([str(KAFKA_TOPICS), "--list", "--bootstrap-server", BOOTSTRAP], check=False).stdout or ""
    if TOPIC not in [ln.strip() for ln in out.splitlines()]:
        run_cmd([
            str(KAFKA_TOPICS), "--create",
            "--bootstrap-server", BOOTSTRAP,
            "--replication-factor", str(REPLICATION),
            "--partitions", str(PARTITIONS),
            "--topic", TOPIC,
        ])

@task
def start_producer() -> int:
    logger = get_run_logger()
    script = PIPELINE_HOME / "kafka_producer.py"
    if not script.exists():
        raise FileNotFoundError(str(script))
    logger.info("📤 Starting Kafka Producer: %s %s", sys.executable, script)
    with open(LOG_FILE, "ab", buffering=0) as lf:
        proc = subprocess.Popen([sys.executable, str(script)],
                                cwd=str(PIPELINE_HOME),
                                stdout=lf, stderr=lf)
    logger.info("✅ Producer PID=%s; logging to %s", proc.pid, LOG_FILE)
    return proc.pid

@flow(name="Kafka Producer Flow")
def producer_flow():
    ensure_kafka_service()
    ensure_topic()
    start_producer()
