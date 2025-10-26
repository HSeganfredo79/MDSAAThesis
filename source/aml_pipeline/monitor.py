import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from neo4j import GraphDatabase
import mlflow
import subprocess
import os, sys
import random

# === CONFIG ===
HALF_LIFE = 86400 * 3  # 3 days in seconds
NUM_BINS = 50
ALERT_P_THRESHOLD = 0.01
MLFLOW_EXPERIMENT = "AML Drift Monitor"

NEO4J_URI = "bolt://192.168.0.5:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "PotatoDTND12!" 

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment(MLFLOW_EXPERIMENT)

reference_histograms = {}
current_histograms = {}
bin_edges = {}
reference_divergences = {}

def fetch_reference_data_from_neo4j(days=7):
    query = f"""
    MATCH (tx:Transaction)
    WHERE tx.enriched_at >= timestamp() - {days * 86400 * 1000}
    RETURN
        tx.amount AS amount,
        tx.from_pagerank AS from_pagerank,
        tx.to_pagerank AS to_pagerank,
        tx.from_centrality AS from_centrality,
        tx.to_centrality AS to_centrality,
        tx.timestamp AS timestamp
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with driver.session() as session:
        result = session.run(query)
        df = pd.DataFrame([r.data() for r in result])
    driver.close()
    return df.dropna()

def stream_last_24h_transactions():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with driver.session() as session:
        # Step 1: get max enriched_at timestamp (in ms)
        result = session.run("""
            MATCH (tx:Transaction)
            WHERE tx.enriched_at IS NOT NULL
            RETURN max(tx.enriched_at) AS latest_enriched
        """).single()
        
        latest_enriched = result["latest_enriched"]
        if latest_enriched is None:
            print("⚠️  No transactions found with enriched_at. Exiting.")
            return pd.DataFrame()

        # Step 2: calculate lower bound for 24h
        lower_bound = int(latest_enriched - 86400000)

        # Step 3: query transactions within 24h window from latest enriched
        query = """
        MATCH (tx:Transaction)
        WHERE tx.enriched_at >= $lower_bound
        RETURN
            tx.amount AS amount,
            tx.from_pagerank AS from_pagerank,
            tx.to_pagerank AS to_pagerank,
            tx.from_centrality AS from_centrality,
            tx.to_centrality AS to_centrality,
            tx.timestamp AS timestamp
        ORDER BY tx.timestamp ASC
        """
        result = session.run(query, lower_bound=lower_bound)
        df = pd.DataFrame([r.data() for r in result])
    driver.close()
    return df.dropna()


def initialize_bins(data_sample):
    for col in data_sample.columns:
        if col != "timestamp":
            bin_edges[col] = np.histogram_bin_edges(data_sample[col], bins=NUM_BINS)
            reference_histograms[col] = np.zeros(NUM_BINS)
            current_histograms[col] = np.zeros(NUM_BINS)
            reference_divergences[col] = []

    for _, row in data_sample.iterrows():
        for col in bin_edges:
            bin_idx = np.digitize(row[col], bin_edges[col]) - 1
            bin_idx = min(max(bin_idx, 0), NUM_BINS - 1)
            reference_histograms[col][bin_idx] += 1

def compute_divergence(p, q):
    # Using L1 Norm (Total Variation Distance): very simple, robust, but less sensitive to fine-grained drift
    return np.sum(np.abs(p - q))

def calculate_p_value(div, history):
    if not history:
        return 1.0
    return 1.0 - (np.searchsorted(sorted(history), div) / len(history))

def update_histograms(event, decay=0.5):
    for col in bin_edges:
        val = event[col]
        bin_idx = np.digitize(val, bin_edges[col]) - 1
        bin_idx = min(max(bin_idx, 0), NUM_BINS - 1)
        current_histograms[col] *= decay
        current_histograms[col][bin_idx] += (1.0 - decay)

def store_drift_in_neo4j(feature, score, timestamp, run_id, is_test=False):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with driver.session() as session:
        session.run(f"""
            MERGE (d:Drift {{feature: $feature, timestamp: $timestamp}})
            SET d.score = $score,
                d.mlflow_run_id = $run_id,
                d.is_drift_test = $is_test,
                d.detected_at = datetime()
        """, feature=feature, score=score, timestamp=timestamp, run_id=run_id, is_test=is_test)
    driver.close()

def trigger_training_cmd():
    print("📅 Triggering CLI train.py")
    sys.stdout.flush()
    subprocess.run(["python3", "train.py"])
    sys.stdout.flush()

def save_overlay_plot(ref_hist, cur_hist, feature, filename):
    x = np.arange(NUM_BINS)
    ref_plot = ref_hist / (ref_hist.sum() + 1e-9)
    cur_plot = cur_hist / (cur_hist.sum() + 1e-9)

    if np.sum(cur_plot) == 0:
        print(f"⚠️  Current histogram for {feature} is empty — skipping plot.")
        return

    plt.figure(figsize=(8, 4))
    plt.bar(x - 0.2, ref_plot, width=0.4, alpha=0.6, label="Reference")
    plt.bar(x + 0.2, cur_plot, width=0.4, alpha=0.6, label="Current")
    plt.title(f"Drift on {feature}")
    plt.xlabel("Bins")
    plt.ylabel("Normalized Frequency")
    plt.grid(True, axis='y', linestyle='--', linewidth=0.5, alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()
    print(f"📊 Saved histogram comparison for {feature} as {filename}")

def monitor_batch(events, is_test=False):
    if not events:
        print("⚠️  No events to monitor.")
        return

    timestamp = int(events[-1]["timestamp"])
    decay_factor = 0.5 ** (1 / HALF_LIFE)

    # Update histograms from all events
    for event in events:
        update_histograms(event, decay=decay_factor)

    drifted_features = []

    with mlflow.start_run(run_name=f"drift_check_{timestamp}") as run:
        for col in bin_edges:
            ref = reference_histograms[col]
            cur = current_histograms[col]
            p = cur / (cur.sum() + 1e-6)
            q = ref / (ref.sum() + 1e-6)
            div = compute_divergence(p, q)
            reference_divergences[col].append(div)
            p_val = calculate_p_value(div, reference_divergences[col][:-1])

            mlflow.log_metric(f"{col}_drift_divergence", div)

            # Save comparative histogram
            hist_path = f"plot_{col}_{timestamp}.png"
            save_overlay_plot(ref, cur, col, hist_path)
            mlflow.log_artifact(hist_path)
            os.remove(hist_path)

            if p_val < ALERT_P_THRESHOLD or is_test:
                print(f"⚠️  Drift detected in: {col}")
                drifted_features.append(col)                
                store_drift_in_neo4j(col, div, timestamp, run.info.run_id, is_test=is_test)

        if is_test:
            mlflow.set_tag("test_drift", True)

        if drifted_features:
            mlflow.set_tag("drift_detected", True)
            mlflow.set_tag("timestamp", timestamp)
            mlflow.set_tag("drifted_features", ",".join(drifted_features))
            trigger_training_cmd()
        else:
            print("✅ No drift detected in this run.")

def inject_test_drift():
    for col in current_histograms:
        print(f"⚠️  Injecting test drift into: {col}")
        current_histograms[col] += np.random.uniform(1, 10, size=NUM_BINS)
        current_histograms[col] /= current_histograms[col].sum()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AML Drift Monitor")
    parser.add_argument("--test-drift", action="store_true", help="Inject test drift into histograms")
    args = parser.parse_args()

    print("🧠 Initializing AML Drift Monitor...")
    df = fetch_reference_data_from_neo4j()
    initialize_bins(df)
    print("✅ Monitoring initialized.")

    if args.test_drift:
        inject_test_drift()
        test_event = {
            "amount": 12345,
            "from_pagerank": 0.85,
            "to_pagerank": 0.91,
            "from_centrality": 0.02,
            "to_centrality": 0.03,
            "timestamp": int(datetime.now().timestamp())
        }
        print(f"📥 Processing with a test transaction to force drift detection...")
        monitor_batch([test_event], is_test=True)
    else:
        tx_df = stream_last_24h_transactions()
        if tx_df.empty:
            print("⚠️  No enriched transactions found. Exiting.")
        else:
            print(f"📥 Processing {len(tx_df)} last enriched transactions (24h block)...")
            monitor_batch(tx_df.to_dict(orient="records"))

