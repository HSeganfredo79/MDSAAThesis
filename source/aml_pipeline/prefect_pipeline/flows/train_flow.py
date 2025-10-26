from prefect import flow, task
from flows.utils import run_script

@task
def run_train(days_ago: int = 30):
    # train.py accepts `[days_ago]` (falls back to default if omitted)
    run_script("train.py", [str(days_ago)])

@flow(name="Weekly Training")
def train_flow(days_ago: int = 30):
    run_train(days_ago)
