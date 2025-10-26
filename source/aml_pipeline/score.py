import os
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from feast import FeatureStore
from neo4j import GraphDatabase
from datetime import datetime, timedelta
import time
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import sys

# ---- Configuration ----
NEO4J_URI = "bolt://192.168.0.5:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "PotatoDTND12!"
MLFLOW_MODEL_NAME = "isolation_forest_model"
FEAST_REPO_PATH = "feast_repo"
BATCH_SIZE = 1000
# path for Spark/Neo4j connection lib
JAR_PATH = os.path.join(os.getcwd(), "lib/neo4j-connector-apache-spark_2.12-4.1.4_for_spark_3.jar")
# Ensure the artifact output folder exists
PLOT_DIR = "plots"
os.makedirs(PLOT_DIR, exist_ok=True)

# Set date to score
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

# Recover transactions from Spark-Neo4j
def fetch_transactions(start_ts, end_ts, sample_limit=None):
    spark = SparkSession.builder \
        .appName("AML Score Pipeline Node") \
        .config("spark.jars", JAR_PATH) \
        .getOrCreate()
    # "/root/tese_henrique/aml_pipeline/lib/neo4j-connector-apache-spark_2.12-4.1.4_for_spark_3.jar"
    print("Spark Session initialized.")

    query = f"""
        CALL {{
            MATCH (tx:Transaction)
            WHERE tx.timestamp >= {start_ts} AND tx.timestamp < {end_ts}
                AND tx.consumed_at IS NOT NULL
                AND tx.enriched_at IS NOT NULL
            RETURN
                tx.transaction_id AS transaction_id,
                tx.amount AS amount,
                tx.from_pagerank AS from_pagerank,
                tx.to_pagerank AS to_pagerank,
                tx.from_centrality AS from_centrality,
                tx.to_centrality AS to_centrality,
                tx.timestamp AS timestamp
            //ORDER BY rand()
            //LIMIT {sample_limit}
        }}
        RETURN *
    """

    df = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", NEO4J_URI) \
        .option("authentication.basic.username", NEO4J_USER) \
        .option("authentication.basic.password", NEO4J_PASS) \
        .option("query", query).load()
    return df.toPandas()

def preprocess(df):
    df = df.dropna(subset=[
        "amount", "from_pagerank", "from_centrality", "to_pagerank", "to_centrality"
    ])
    features = df[[
        "amount", "from_pagerank", "from_centrality", "to_pagerank", "to_centrality"
    ]]
    return df, features

def write_batch_to_neo4j(driver, batch_df, run_id):
    query = """
    UNWIND $rows AS row
    MATCH (tx:Transaction {transaction_id: row.transaction_id})
    SET tx.scoring = row.scoring,
        tx.label = row.label,
        tx.scored_at = row.scored_at,
        tx.model_run_id = row.model_run_id
    """
    rows = []
    for _, row in batch_df.iterrows():
        rows.append({
            "transaction_id": row["transaction_id"],
            "scoring": float(row["scoring"]),
            "label": int(row["label"]),
            "scored_at": int(row["scored_at"]),
            "model_run_id": run_id
        })
    with driver.session() as session:
        session.write_transaction(lambda tx: tx.run(query, rows=rows))

def log_score_distribution(scores, title="Isolation Forest Score Distribution", filename="score_distribution.png"):
    os.makedirs(PLOT_DIR, exist_ok=True)
    plot_path = os.path.join(PLOT_DIR, filename)

    # Plot with threshold line
    plt.figure(figsize=(8, 4))
    plt.hist(scores, bins=50, color='skyblue', edgecolor='black')
    plt.axvline(0, color='red', linestyle='--', label='Threshold (score = 0)')
    plt.title(title)
    plt.xlabel("Score")
    plt.ylabel("Frequency")
    plt.legend()
    plt.tight_layout()
    plt.savefig(plot_path)
    mlflow.log_artifact(plot_path)
    plt.close()

    # Log score percentiles
    percentiles = {
        "score_min": np.min(scores),
        "score_25": np.percentile(scores, 25),
        "score_50": np.median(scores),
        "score_75": np.percentile(scores, 75),
        "score_max": np.max(scores),
    }

    for key, value in percentiles.items():
        mlflow.log_metric(key, round(value, 6))

    print(f"✅ Score distribution plot logged to MLflow: {plot_path}")
    print(f"📊 Percentiles logged: {percentiles}")


# ---- Main Pipeline ----
mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("AML Isolation Forest Inference")

with mlflow.start_run(run_name="score-daily-partitioned") as run:
    model = mlflow.sklearn.load_model(f"models:/{MLFLOW_MODEL_NAME}/Production")
    model_run_id = run.info.run_id
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

    input_date, start_ts, end_ts = get_target_day_timestamps()

    df = fetch_transactions(start_ts, end_ts)
    if df.empty:
        print(f"❌ No transactions were found in day {input_date} to score!")

    entity_df = pd.DataFrame({
        "transaction_id": df["transaction_id"],
        "event_timestamp": pd.to_datetime(df["timestamp"], unit="s"),
    })

    try:
        store = FeatureStore(FEAST_REPO_PATH)
        feast_df = store.get_historical_features(
            entity_df=entity_df,
            features=[
                "aml_features:from_pagerank",
                "aml_features:from_centrality",
                "aml_features:to_pagerank",
                "aml_features:to_centrality",
                "aml_features:amount"
            ]).to_df()
        features_df = feast_df.drop(columns=["event_timestamp", "transaction_id"])
    except Exception:
        df, features_df = preprocess(df)

    # enforce column order to match training
    expected_cols = ["amount", "from_pagerank", "to_pagerank", "from_centrality", "to_centrality"]
    features_df = features_df[expected_cols]

    # sanity check
    missing = set(expected_cols) - set(features_df.columns)
    extra = set(features_df.columns) - set(expected_cols)
    if missing or extra:
        raise ValueError(f"❌ Column mismatch.\nMissing: {missing}\nExtra: {extra}")
    
    # predict with IsolationForest
    scores = model.decision_function(features_df)
    labels = model.predict(features_df) 
    anomalies = (labels == -1).astype(int) # set 0=normal, 1=anomaly (usual output is -1 for anomalies flagged and 1 for normals)

    result_df = pd.DataFrame({
        "transaction_id": df["transaction_id"],
        "scoring": scores,
        "label": anomalies,
        "scored_at": int(datetime.utcnow().timestamp())
    })

    log_score_distribution(scores) # log distribution figure in mlflow
    mlflow.log_metric(f"day_{input_date}_avg_score", np.mean(scores))
    mlflow.log_metric(f"day_{input_date}_anomaly_ratio", np.mean(anomalies))

    # slow code...
    # CREATE INDEX transaction_id_index IF NOT EXISTS FOR (tx:Transaction) ON (tx.transaction_id)
    for start in range(0, len(result_df), BATCH_SIZE):
        batch_df = result_df.iloc[start:start + BATCH_SIZE]
        write_batch_to_neo4j(driver, batch_df, model_run_id)

    print(f"✅ Day : Scored {len(result_df)} transactions")

    driver.close()
 
