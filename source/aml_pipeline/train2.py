"""
train2.py — Isolation Forest training (v2) using phase-1 + phase-2 (v2) features.

WHY add SDV/CTGAN augmentation to an unsupervised method?
---------------------------------------------------------
- Isolation Forest partitions feature space using random trees. If the benign (majority) manifold
  is sparse (holes/gaps), truly atypical points may not be clearly isolated because the forest
  doesn't "see" a continuous normal region to contrast against.
- CTGAN learns the joint distribution of  training features and can generate synthetic
  but realistic samples that densify the benign manifold. This makes the normal region more
  continuous and the genuine outliers comparatively easier to isolate.
- Practically: we fit CTGAN on a slice of  real data, generate a modest amount of synthetic
  points (e.g., ~10% of the sample), and train IF on the union. This is not class-imbalance
  augmentation (we still have no labels); it’s manifold densification to help an unsupervised
  detector form cleaner boundaries.

WHAT we train on (features)
---------------------------
We combine:
  • Original graph context features from phase-1:
      amount, from_pagerank, to_pagerank, from_centrality, to_centrality
  • New phase-2 enrichment features:
      funneling_score_v2, funneling_multiple_senders_v2,
      smurfing_score_v2, smurfing_small_tx_count_v2

These v2 features summarize intra-day network patterns (WHY suspicious, WHERE they arise),
helping the model separate benign vs abnormal traffic without labels.

WHEN & WHERE (windowing)
------------------------
We train on a 30-day sliding window that ENDS at the day you pass (YYYYMMDD).
Window is [start, end) in UTC epoch seconds:
  start = (end_of_day - 30 days), end = (YYYYMMDD + 1 day at 00:00Z)
This balances recency (drift) with having enough coverage for an unsupervised model.

HOW (pipeline)
--------------
1) Parse input day (YYYYMMDD) or a single integer "days_ago".
2) Build [start_ts, end_ts) for a 30-day window ending that day.
3) Pull transactions from Neo4j that are at least phase-1 enriched; coalesce v2 fields if absent.
4) Preprocess: numeric guardrails + StandardScaler (center/scale improves IF split geometry).
5) (NEW) SDV/CTGAN: fit CTGAN on a real slice and synthesize ~10% extra samples to densify the benign manifold.
6) Train Isolation Forest on real+synthetic (scaled).
7) Log model, scaler, and metadata to MLflow; register under "isolation_forest_v2".
"""

import os
import sys
import json
import math
import time
import pickle
import logging
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest

from neo4j import GraphDatabase
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

# ----- SDV/CTGAN (optional import; we handle absence gracefully) -----
SDV_AVAILABLE = True
try:
    from sdv.single_table import CTGANSynthesizer
    from sdv.metadata import SingleTableMetadata
except Exception:
    SDV_AVAILABLE = False

# ----------------- CONFIG -----------------
NEO4J_URI  = os.getenv("NEO4J_URI",  "bolt://192.168.0.5:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "PotatoDTND12!")

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_EXPERIMENT   = os.getenv("MLFLOW_EXPERIMENT",   "AML Isolation Forest Train phase 2")
MLFLOW_MODEL_NAME   = os.getenv("MLFLOW_MODEL_NAME_V2","isolation_forest_model_phase2")

LOG_DIR = os.getenv("LOG_DIR", "/var/log/aml_pipeline")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "train2.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Sliding window length (days) used for each train run (ending at the input day)
# Spark implementation left out due memory contraints to allow 30=day range (training for phase 1 done withing 10 days only)
WINDOW_DAYS = int(os.getenv("TRAIN2_WINDOW_DAYS", "30"))

# Isolation Forest hyperparameters (sane defaults; adjust via env if desired)
IF_N_ESTIMATORS = int(os.getenv("IF_N_ESTIMATORS", "200"))
IF_CONTAM       = os.getenv("IF_CONTAMINATION", 0.05)  # follow AML practice (BIS paper)
IF_RANDOM_STATE = int(os.getenv("IF_RANDOM_STATE", "42")) # same as train v1

# Optional sampling (e.g., train on a subset for speed). Set to "1.0" to use all rows.
SAMPLE_FRACTION = float(os.getenv("TRAIN2_SAMPLE_FRACTION", "1.0"))

# SDV/CTGAN controls
CTGAN_EPOCHS      = int(os.getenv("TRAIN2_CTGAN_EPOCHS", "50"))
CTGAN_BATCH_SIZE  = int(os.getenv("TRAIN2_CTGAN_BATCH_SIZE", "500"))
SYNTH_FRACTION    = float(os.getenv("TRAIN2_SYNTH_FRACTION", "0.10"))  # 10% of *real_sample* size
REAL_SAMPLE_FRAC  = float(os.getenv("TRAIN2_REAL_SAMPLE_FRAC", "0.10"))  # 10% real to fit CTGAN/train

# ----------------- INPUT PARSING -----------------
def parse_input_arg(arg: str):
    """
    Accepts either:
      - YYYYMMDD (preferred): train window ends at this day (exclusive end at next midnight UTC)
      - integer 'days_ago' (legacy): window ends at (now - days_ago + 1 day)
    Returns (start_ts, end_ts, label_string_for_logging)
    """
    def ts_of_yyyymmdd(s: str) -> int:
        if len(s) == 8 and s.isdigit():
            dt = datetime.strptime(s, "%Y%m%d").replace(tzinfo=timezone.utc)
            end = dt + timedelta(days=1)  # exclusive end boundary
            return int(end.timestamp())
        raise ValueError("Not in YYYYMMDD format")

    now = datetime.now(timezone.utc)
    try:
        end_ts = ts_of_yyyymmdd(arg)
        end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc)
        start_ts = int((end_dt - timedelta(days=WINDOW_DAYS)).timestamp())
        label = f"YYYYMMDD={arg}"
        return start_ts, end_ts, label
    except Exception:
        try:
            days_ago = int(arg)
        except ValueError as e:
            raise SystemExit(f"Argument '{arg}' is neither YYYYMMDD nor integer days_ago") from e
        end_dt = (now - timedelta(days=days_ago-1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_ts = int(end_dt.timestamp())
        start_ts = int((end_dt - timedelta(days=WINDOW_DAYS)).timestamp())
        label = f"days_ago={days_ago}"
        return start_ts, end_ts, label

# ----------------- DATA ACCESS -----------------
def fetch_transactions(tx, start_ts: int, end_ts: int) -> pd.DataFrame:
    """
    Fetch transactions from Neo4j within [start_ts, end_ts) that have at least phase-1 enrichment.
    We coalesce v2 fields to keep training robust on older days (0.0 or 0 fallbacks).

    NOTE: We read only what we train on — keep this light to avoid unnecessary data transfer.
    """
    q = """
    MATCH (t:Transaction)
    WHERE t.timestamp >= $start_ts AND t.timestamp < $end_ts
      AND t.enriched_at IS NOT NULL
    RETURN
       t.transaction_id AS transaction_id,
       coalesce(t.amount, 0.0)            AS amount,
       coalesce(t.from_pagerank, 0.0)     AS from_pagerank,
       coalesce(t.to_pagerank, 0.0)       AS to_pagerank,
       coalesce(t.from_centrality, 0.0)   AS from_centrality,
       coalesce(t.to_centrality, 0.0)     AS to_centrality,
       coalesce(t.funneling_score_v2, 0.0)            AS funneling_score_v2,
       coalesce(t.funneling_multiple_senders_v2, 0)   AS funneling_multiple_senders_v2,
       coalesce(t.smurfing_score_v2, 0.0)             AS smurfing_score_v2,
       coalesce(t.smurfing_small_tx_count_v2, 0)      AS smurfing_small_tx_count_v2
    """
    rows = tx.run(q, start_ts=start_ts, end_ts=end_ts)
    data = [dict(r) for r in rows]
    return pd.DataFrame(data)

# ----------------- PREPROCESS -----------------
def build_feature_matrix(df: pd.DataFrame):
    """
    WHY: Isolation Forest is distance/tree-based. Standardizing features keeps splits
         more stable when feature scales differ (amount vs. pagerank vs. counts).
    WHAT: build a fixed feature order, cast to float, StandardScaler().fit_transform().
    WHERE: all in-memory; datasets are moderate per run thanks to windowing and optional sampling.
    HOW: features[] -> scaler.fit_transform(features) -> return X and artifacts.
    """
    if df.empty:
        raise SystemExit("No training data returned for the selected window.")

    # Stable feature order (phase 1 first, then phase 2 "v2"); keep update-friendly for downstream scoring
    feature_cols = [
        "amount",
        "from_pagerank", "to_pagerank",
        "from_centrality", "to_centrality",
        "funneling_score_v2", "funneling_multiple_senders_v2",
        "smurfing_score_v2", "smurfing_small_tx_count_v2",
    ]

    # Defensive cleaning / typing
    for c in feature_cols:
        if c not in df.columns:
            df[c] = 0.0
    features = df[feature_cols].astype(float)

    # Optional sampling (primarily for speed on very large windows)
    #if 0.0 < SAMPLE_FRACTION < 1.0:
    #    df = df.sample(frac=SAMPLE_FRACTION, random_state=42).reset_index(drop=True)
    #    features = df[feature_cols].astype(float)

    # using scaler, need to be consistent on scoring
    scaler = StandardScaler()
    X = scaler.fit_transform(features)

    return df, X, scaler, feature_cols

# ----------------- SDV/CTGAN AUGMENTATION -----------------
def ctgan_augment(df: pd.DataFrame, feature_cols, real_sample_frac=REAL_SAMPLE_FRAC,
                  synth_fraction=SYNTH_FRACTION, epochs=CTGAN_EPOCHS, batch_size=CTGAN_BATCH_SIZE) -> pd.DataFrame:
    """
    WHY (again): Densify the benign manifold of the joint feature distribution, so Isolation Forest
                 has a clearer 'normal' region and isolates true outliers more confidently.

    WHAT:
      1) Take a real sample (e.g., 10% of training rows) restricted to the feature columns.
      2) Fit a CTGAN generator on that sample (learn the joint distribution).
      3) Generate a modest amount of synthetic rows (e.g., 10% of the real_sample size).
      4) Concatenate (real ∪ synthetic) -> return augmented frame.

    WHERE:
      All in-memory, before scaling for IF training.

    HOW:
      - Use SDV SingleTableMetadata to declare all selected feature columns as numerical.
      - Instantiate CTGANSynthesizer(metadata, epochs, batch_size), fit on the sample,
        and call .sample(n) to produce synthetic rows.
      - If SDV is missing at runtime, gracefully return the real sample only and warn.
    """
    n_total = len(df)
    if n_total == 0:
        return df.copy()

    # 1) Real sample to fit CTGAN and also to contribute real rows to training
    n_real = max(1, int(real_sample_frac * n_total))
    real_sample = df.sample(n=n_real, random_state=42)[feature_cols].reset_index(drop=True)

    if not SDV_AVAILABLE:
        logging.warning("SDV/CTGAN not available. Proceeding with real sample only (no synthetic augmentation).")
        return real_sample

    # 2) Build SDV metadata (all columns numeric continuous)
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(data=real_sample)
    # Force numeric dtype (guard against any inference quirks)
    for col in feature_cols:
        if col not in metadata.columns:
            metadata.add_column(col_name=col, sdtype="numerical")

    # 3) Fit CTGAN on the real sample
    synth = CTGANSynthesizer(metadata, epochs=epochs, batch_size=batch_size, verbose=True)
    synth.fit(real_sample)

    # 4) Generate synthetic rows
    n_synth = max(1, int(synth_fraction * n_real))
    synthetic_df = synth.sample(num_rows=n_synth)[feature_cols].astype(float)

    # 5) Union and return
    augmented = pd.concat([real_sample, synthetic_df], axis=0, ignore_index=True)
    logging.info("CTGAN augmentation: real=%d, synth=%d -> augmented=%d", len(real_sample), len(synthetic_df), len(augmented))
    return augmented

# ----------------- TRAIN -----------------
def train_if(X: np.ndarray) -> IsolationForest:
    """
    Train Isolation Forest (unsupervised). We keep n_estimators large enough to
    stabilize path-length statistics while maintaining reasonable runtime.
    """
    clf = IsolationForest(
        n_estimators=IF_N_ESTIMATORS,
        contamination=IF_CONTAM,   # keep "auto" unless you know the anomaly rate
        random_state=IF_RANDOM_STATE,
        n_jobs=-1
    )
    clf.fit(X)
    return clf

# ----------------- MLflow LOGGING -----------------
def log_and_register(model: IsolationForest, scaler: StandardScaler, feature_cols, run_name: str,
                     extras: dict = None):
    """
    Log all artifacts to MLflow and register the model under MLFLOW_MODEL_NAME.
    WHY: Reproducibility (data drift audits), lineage, and easy rollbacks.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name=run_name) as run:
        # Params/metadata
        mlflow.log_param("window_days", WINDOW_DAYS)
        mlflow.log_param("sample_fraction", SAMPLE_FRACTION)
        mlflow.log_param("n_estimators", IF_N_ESTIMATORS)
        mlflow.log_param("contamination", IF_CONTAM)
        mlflow.log_param("feature_count", len(feature_cols))
        mlflow.log_dict({"feature_order": list(feature_cols)}, "feature_order.json")
        mlflow.log_param("sdv_available", SDV_AVAILABLE)
        mlflow.log_param("real_sample_frac", REAL_SAMPLE_FRAC)
        mlflow.log_param("synth_fraction", SYNTH_FRACTION)
        mlflow.log_param("ctgan_epochs", CTGAN_EPOCHS)
        mlflow.log_param("ctgan_batch_size", CTGAN_BATCH_SIZE)

        if extras:
            for k, v in extras.items():
                mlflow.log_param(k, v)

        # Save scaler (for inference parity) and model
        os.makedirs("/tmp/train2_artifacts", exist_ok=True)
        scaler_path = "/tmp/train2_artifacts/scaler.pkl"
        with open(scaler_path, "wb") as f:
            pickle.dump(scaler, f)
        mlflow.log_artifact(scaler_path) #, artifact_path=""

        mlflow.sklearn.log_model(model, artifact_path="isolation_forest_v2")

        # Register to Model Registry
        result = mlflow.register_model(
            model_uri=f"runs:/{run.info.run_id}/isolation_forest_v2",
            name=MLFLOW_MODEL_NAME
        )
        
        client = MlflowClient()

        # Wait until model version is READY
        for _ in range(60):  # up to ~60s
            mv = client.get_model_version(name=MLFLOW_MODEL_NAME, version=result.version)
            if getattr(mv, "status", None) == "READY":
                break
            time.sleep(1)

        # Promote to Production (archive old ones)
        client.transition_model_version_stage(
            name=MLFLOW_MODEL_NAME,
            version=result.version,
            stage="Production",
            archive_existing_versions=True
        )

    print(f"✅ Model registered and promoted to Production: {MLFLOW_MODEL_NAME} v{result.version}")

    return run.info.run_id, result.version

# ----------------- MAIN -----------------
def main():
    if len(sys.argv) < 2:
        print("Usage:\n  python3 train2.py YYYYMMDD\n  (or legacy) python3 train2.py <days_ago_int>", file=sys.stderr)
        sys.exit(2)

    # Parse either YYYYMMDD or days_ago
    start_ts, end_ts, label = parse_input_arg(sys.argv[1])
    end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    logging.info("Training window [%s, %s) %s", start_dt.isoformat(), end_dt.isoformat(), label)

    # Neo4j session
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with driver.session() as session:
        df = session.read_transaction(fetch_transactions, start_ts, end_ts)

    if df.empty:
        logging.error("No data fetched for window [%s, %s). Aborting.", start_dt, end_dt)
        sys.exit(1)

    # Build feature matrix + scaler from ALL real rows (so scaling uses the full manifold)
    df, X_real, scaler, feature_cols = build_feature_matrix(df)

    # SDV/CTGAN augmentation (densify benign manifold). This happens BEFORE scaling for training.
    # We generate synthetic rows with the same feature schema and then scale them using the
    # scaler fitted on REAL data (very important: synthetic should not drive the scaling).
    augmented_df = ctgan_augment(df, feature_cols)
    if augmented_df is df:
        # SDV not available (or no rows). Fall back to real-only.
        X_train = X_real
        extras = {"augmented_rows": 0, "train_rows": int(X_train.shape[0])}
    else:
        # Scale augmented data using the REAL-data scaler
        X_synth = scaler.transform(augmented_df[feature_cols].astype(float))
        # IMPORTANT: we want the *union* of real + synthetic for training, not just synthetic.
        X_train = np.vstack([X_real, X_synth])
        extras = {
            "augmented_rows": int(len(augmented_df)),
            "train_rows": int(X_train.shape[0]),
            "real_rows": int(X_real.shape[0]),
            "synth_rows": int(len(augmented_df) - len(augmented_df) // 2)  # rough; see log for exact numbers
        }

    # Train IF
    t0 = time.time()
    model = train_if(X_train)
    train_secs = time.time() - t0
    logging.info("Isolation Forest fit in %.2f sec on %d rows", train_secs, X_train.shape[0])

    # Log + register
    run_name = f"train2_{label}_{end_dt.strftime('%Y%m%d')}"
    extras["fit_seconds"] = round(train_secs, 2)
    run_id, version = log_and_register(model, scaler, feature_cols, run_name, extras=extras)
    logging.info("✅ Training v2 complete. run_id=%s, registry_version=%s", run_id, version)
    print("✅ Training v2 complete. run_id:", run_id, "version:", version)

if __name__ == "__main__":
    main()
