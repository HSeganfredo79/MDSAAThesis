#!/usr/bin/env python3
"""
score2.py — Isolation Forest inference (v2) using phase-1 + phase-2 (v2) features.

WHY (business/scientific rationale)
-----------------------------------
Money-laundering typologies evolve quickly; labeled ground truth is scarce, delayed, and
often biased toward historical detection patterns. An *unsupervised* model such as Isolation Forest
remains relevant and useful because:

1) It does not require labels and therefore scales across new data and new jurisdictions.
2) It captures "unknown unknowns": previously unseen behaviors that deviate from the dominant
   population (heavy tails, multi-modal clusters, rare subgraph behaviors).
3) It complements supervised components we may add later (e.g., case outcomes) and provides
   a robust prior score that our scoring step can use downstream or ensemble with other detectors.

Scoring script for the v2 model. Key properties:
- Loads model AML_IF_v2 from MLflow Model Registry (Model Name is configurable)
- Retrieves features including funneling_* and smurfing_* and uses them (keeps the original scoring fields untouched)
- Writes back new fields:
    tx.scoring_v2
    tx.label_v2
    tx.model_run_id_v2
    tx.scored_v2_at
- Given up on using Feast due on in-database features.
"""

import os, logging, tempfile, pickle
import pandas as pd
import numpy as np
import mlflow
from mlflow.tracking import MlflowClient
from datetime import datetime
from neo4j import GraphDatabase
#from feast import FeatureStore
from datetime import datetime, timedelta
import sys

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://192.168.0.5:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "PotatoDTND12!")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME_V2 = os.getenv("MLFLOW_MODEL_NAME_V2", "isolation_forest_model_phase2")
#FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "./feature_repo")

LOG_DIR = os.getenv("LOG_DIR", "/var/log/aml_pipeline")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(filename=os.path.join(LOG_DIR, "score2.log"), level=logging.INFO)

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
client = MlflowClient(MLFLOW_TRACKING_URI)
mlflow.set_experiment("AML Isolation Forest Inference phase 2")

# Same column order as training, no need for extra mapping
FEATURE_ORDER = [
    "amount",
    "from_pagerank",
    "to_pagerank",
    "from_centrality",
    "to_centrality",
    "funneling_score_v2",
    "funneling_multiple_senders_v2",
    "smurfing_score_v2",
    "smurfing_small_tx_count_v2"
]

def get_target_day_timestamps():
    try:
        input_date = sys.argv[1]
        date = datetime.strptime(input_date, "%Y%m%d")
    except (IndexError, ValueError):
        print("❌ Please provide a date in format YYYYMMDD, e.g., 20250414")
        sys.exit(1)

    start_ts = int(datetime.combine(date, datetime.min.time()).timestamp())
    end_ts = int((datetime.combine(date, datetime.min.time()) + timedelta(days=1)).timestamp())
    print(f"🔍 Scoring transactions from {date.strftime('%Y-%m-%d')} ({start_ts} to {end_ts})...")
    return input_date, start_ts, end_ts

def fetch_transactions(start_ts, end_ts, sample_limit=None):
    """
    No spark implementation (differs from phase 1 scoring)
    """
    # Fallback to direct cypher fetch
    with driver.session() as session:
        q = f"""
        MATCH (tx:Transaction)
        WHERE tx.timestamp >= {start_ts} AND tx.timestamp < {end_ts}
          AND tx.consumed_at IS NOT NULL
          AND tx.enriched_at IS NOT NULL
          AND tx.enriched_v2_at IS NOT NULL
        RETURN tx.transaction_id AS transaction_id,
               tx.amount AS amount,
               tx.from_pagerank AS from_pagerank,
               tx.to_pagerank AS to_pagerank,
               tx.from_centrality AS from_centrality,
               tx.to_centrality AS to_centrality,
               tx.funneling_score_v2 AS funneling_score_v2,
               tx.funneling_multiple_senders_v2 AS funneling_multiple_senders_v2,
               tx.smurfing_score_v2 AS smurfing_score_v2,
               tx.smurfing_small_tx_count_v2 AS smurfing_small_tx_count_v2,
               tx.timestamp AS timestamp
        """
        res = session.run(q)
        rows = [dict(r) for r in res]
        return pd.DataFrame(rows)

def preprocess(df):
    # Mirror the training order.
    df = df.dropna(subset=FEATURE_ORDER)  # require v2 features
    X = df[FEATURE_ORDER].astype(float)
    return df, X

def load_model_v2():
    # Load latest Production stage model for v2 name
    model_uri = f"models:/{MODEL_NAME_V2}/Production"
    mv = client.get_latest_versions(name=MODEL_NAME_V2, stages=["Production"])[0]
    try:
        model = mlflow.sklearn.load_model(model_uri)
        logging.info("Loaded model: %s", model_uri)
        # get scaler saved by train2
        with tempfile.TemporaryDirectory() as td:
            local = client.download_artifacts(mv.run_id, "scaler.pkl", td)
            with open(local, "rb") as f:
                scaler = pickle.load(f)
        return model, scaler
    except Exception as e:
        logging.error("Failed to load model %s: %s", model_uri, e)
        raise

def write_scores_back(driver, rows, run_id):
    q = """
    UNWIND $rows AS r
    MATCH (t:Transaction {transaction_id: r.transaction_id})
    SET t.scoring_v2 = r.scoring_v2,
        t.label_v2 = r.label_v2,
        t.model_run_id_v2 = r.model_run_id_v2,
        t.scored_v2_at = r.scored_v2_at
    """
    with driver.session() as s:
        # chunk to avoid giant transactions
        BATCH = 500
        for i in range(0, len(rows), BATCH):
            chunk = rows[i:i+BATCH]
            s.run(q, rows=chunk)

if __name__ == "__main__":
    input_date, start_ts, end_ts = get_target_day_timestamps()
    df = fetch_transactions(start_ts, end_ts)
    if df.empty:
        logging.info("No transactions found for date %s", input_date)
        print("No transactions found; exiting.")
        exit(0)

    df, X = preprocess(df)
    X = X.values #estimator was trained without names
    model, scaler = load_model_v2()
    # scale data
    X_scaled = scaler.transform(X)
    
    # predict with IsolationForest
    scores = model.decision_function(X_scaled)
    labels = model.predict(X_scaled)  
    anomalies = (labels == -1).astype(int) # set 0=normal, 1=anomaly (usual output is -1 for anomalies flagged and 1 for normals)

    result_rows = []
    run_id = f"score2_{int(datetime.utcnow().timestamp())}"
    now_ts = int(datetime.utcnow().timestamp())
    for txid, sc, lb in zip(df["transaction_id"], scores, anomalies):
        result_rows.append({
            "transaction_id": txid,
            "scoring_v2": float(sc),
            "label_v2": int(lb),
            "model_run_id_v2": run_id,
            "scored_v2_at": now_ts
        })

    write_scores_back(driver, result_rows, run_id)
    logging.info("✅ Scored %d transactions (phase 2)", len(result_rows))
    print("✅ Scored", len(result_rows), "transactions (phase 2)")
