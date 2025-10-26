import os, mlflow, time
from mlflow.tracking import MlflowClient

MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_URI)
client = MlflowClient()

MODEL_NAME = "isolation_forest_model"   # <-- change if needed
RUN_IDS = [
    "b5bef0c50ae048658dc41d1b7a9c4eb2",
    "909cd0309aaf43fc911591dbda0c81a3",
]

def pick_version_by_time(model_name, cutoff_ms):
    """Best-effort: prefer a version whose last_updated <= cutoff and stage=Production/Staging.
       Else choose the latest version whose creation <= cutoff. Else return latest overall."""
    mvs = client.search_model_versions(f"name='{model_name}'")
    if not mvs: return None
    # to ints
    for mv in mvs:
        mv._creation = int(mv.creation_timestamp or 0)
        mv._updated  = int(mv.last_updated_timestamp or mv._creation)
        mv._stage    = (mv.current_stage or "").lower()

    # 1) prefer prod/staging updated before cutoff
    staged = [mv for mv in mvs if mv._updated <= cutoff_ms and mv._stage in ("production", "staging")]
    if staged:
        return sorted(staged, key=lambda x: (x._updated, int(x.version)))[-1]
    # 2) any version created before cutoff
    older = [mv for mv in mvs if mv._creation <= cutoff_ms]
    if older:
        return sorted(older, key=lambda x: (x._creation, int(x.version)))[-1]
    # 3) fallback to latest overall
    return sorted(mvs, key=lambda x: int(x.version))[-1]

def backfill_tags_for_run(run_id, model_name):
    run = client.get_run(run_id)  # raises if missing
    cutoff = int(run.info.end_time or run.info.start_time or int(time.time()*1000))
    mv = pick_version_by_time(model_name, cutoff)
    if not mv:
        print(f"{run_id}: no versions for model '{model_name}' found")
        return
    # Write tags so your jobs/notebooks can resolve model details easily
    client.set_tag(run_id, "registered_model_name", model_name)
    client.set_tag(run_id, "registered_model_version", str(mv.version))
    client.set_tag(run_id, "registered_model_stage", mv.current_stage or "None")
    client.set_tag(run_id, "registered_model_matched_at", str(int(time.time()*1000)))
    client.set_tag(run_id, "registered_model_match_rule", "approx_time")
    print(f"{run_id}: tagged -> {model_name} v{mv.version} ({mv.current_stage or 'None'})")

for rid in RUN_IDS:
    try:
        backfill_tags_for_run(rid, MODEL_NAME)
    except Exception as e:
        print(f"{rid}: error {e}")
