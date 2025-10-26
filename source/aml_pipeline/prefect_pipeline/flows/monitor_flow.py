from prefect import flow, task
from flows.utils import run_script

@task
def run_monitor():
    # monitor.py uses argparse; runs fine with no args (or add args in the list if you use any)
    run_script("monitor.py", [])

@flow(name="Drift Monitor")
def monitor_flow():
    run_monitor()
