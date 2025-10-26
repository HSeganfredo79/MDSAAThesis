
# Adapted train.py — accepts EITHER a date range "YYYYMMDD-YYYYMMDD" OR an integer "days_back".
# Nothing else is accepted.
#
# Usage examples:
#   python3 train.py 20250401-20250430
#   python3 train.py 30
#
from pyspark.sql import SparkSession
from sklearn.ensemble import IsolationForest
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import evaluate_quality
from mlflow.tracking import MlflowClient
import mlflow, mlflow.sklearn
import pandas as pd
from datetime import datetime, timedelta, timezone
import time, os, sys
import neo4j

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("AML Isolation Forest Train")
PLOT_DIR, REPORT_DIR, METADATA_DIR = "plots", "reports", "metadata"
os.makedirs(PLOT_DIR, exist_ok=True); os.makedirs(REPORT_DIR, exist_ok=True); os.makedirs(METADATA_DIR, exist_ok=True)
JAR_PATH = os.path.join(os.getcwd(), "lib/neo4j-connector-apache-spark_2.12-4.1.4_for_spark_3.jar")

def parse_arg_to_window(arg: str):
    # Range YYYYMMDD-YYYYMMDD
    if '-' in arg and len(arg) == 17:
        start_str, end_str = arg.split('-')
        if len(start_str)==8 and start_str.isdigit() and len(end_str)==8 and end_str.isdigit():
            start_dt = datetime.strptime(start_str, "%Y%m%d").replace(tzinfo=timezone.utc)
            end_dt_excl = (datetime.strptime(end_str, "%Y%m%d") + timedelta(days=1)).replace(tzinfo=timezone.utc)
            return int(start_dt.timestamp()), int(end_dt_excl.timestamp()), f"range={start_str}-{end_str}"
    # Integer days_back
    try:
        days_back = int(arg)
        driver = neo4j.GraphDatabase.driver("bolt://192.168.0.5:7687", auth=("neo4j", "PotatoDTND12!"))
        with driver.session() as session:
            row = session.run("MATCH (tx:Transaction) WHERE tx.enriched_at IS NOT NULL RETURN max(tx.timestamp) AS latest_ts").single()
        latest = int(row["latest_ts"]) if row and row["latest_ts"] is not None else int(datetime.now(timezone.utc).timestamp())
        start_ts = latest - 86400*days_back
        return start_ts, latest, f"days_back={days_back}"
    except Exception:
        raise SystemExit("Argument must be either 'YYYYMMDD-YYYYMMDD' or integer days_back")

def fetch_transactions(start_ts, end_ts, sample_limit=None):
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
                tx.to_centrality AS to_centrality
        }}
        RETURN *
    """
    spark = (
        SparkSession.builder
        .appName("AML Train Pipeline Node")
        .config("spark.jars", JAR_PATH)
        .getOrCreate()
    )
    df = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", "bolt://192.168.0.5:7687")
        .option("authentication.type", "basic")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "PotatoDTND12!")
        .option("query", query)
        .load()
    )
    print(f"Transactions fetched: {df.count()} records.")
    return df

def preprocess(df):
    pdf = df.toPandas()
    scaled_df = pdf[['transaction_id','amount','from_pagerank','to_pagerank','from_centrality','to_centrality']].copy()
    return scaled_df, None

def generate_synthetic_data(df, synthetic_count):
    print(f"Generating {synthetic_count} synthetic data samples using SDV CTGANSynthesizer...")
    train_df = df.drop(columns=['transaction_id'])
    metadata = SingleTableMetadata(); metadata.detect_from_dataframe(data=train_df)
    model = CTGANSynthesizer(metadata, epochs=50, verbose=True); model.fit(train_df)
    synthetic_scaled = model.sample(synthetic_count)
    synthetic_data = pd.DataFrame(df['transaction_id'].sample(n=synthetic_count, replace=True).values, columns=['transaction_id'])
    synthetic_data = pd.concat([synthetic_data.reset_index(drop=True), synthetic_scaled.reset_index(drop=True)], axis=1)
    print("Synthetic data generation completed using SDV.")
    return synthetic_data

def train_isolation_forest(real_data, synthetic_data):
    with mlflow.start_run() as run:
        start_time = time.time()
        combined = pd.concat([real_data, synthetic_data])
        features = combined.drop(columns=['transaction_id'])
        isolation_forest = IsolationForest(contamination=0.05, random_state=42) # follow AML practice on BIS paper (0.05 contamination)
        isolation_forest.fit(features)
        mlflow.log_param("synthetic_sample_size", len(synthetic_data))
        mlflow.log_param("real_sample_size", len(real_data))
        mlflow.log_metric("training_time_sec", time.time() - start_time)
        mlflow.log_metric("synthetic_to_real_ratio", len(synthetic_data) / max(1,len(real_data)))
        mlflow.log_dict({"feature_order": features.columns.tolist()}, "feature_order.json")
        mlflow.sklearn.log_model(isolation_forest, "isolation_forest_model", input_example=features.iloc[:5])
        try:
            metadata = SingleTableMetadata(); metadata.detect_from_dataframe(real_data.drop(columns=['transaction_id']))
            quality_report = evaluate_quality(real_data.drop(columns=['transaction_id']), synthetic_data.drop(columns=['transaction_id']), metadata)
            report_path = os.path.join("reports", "quality_report.pkl"); quality_report.save(filepath=report_path); mlflow.log_artifact(report_path)
        except Exception as e:
            print("Skipping SDV quality report:", e)
        print("✅ MLflow logging complete.")
    return isolation_forest, run.info.run_id

def register_model_with_mlflow(run_id, model_name="isolation_forest_model"):
    result = mlflow.register_model(model_uri=f"runs:/{run_id}/{model_name}", name=model_name)
    client = MlflowClient()
    client.transition_model_version_stage(name=model_name, version=result.version, stage="Production", archive_existing_versions=True)
    print(f"✅ Model registered and promoted to Production: {model_name} v{result.version}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 train.py YYYYMMDD-YYYYMMDD  |  python3 train.py <days_back_int>", file=sys.stderr); sys.exit(2)
    start_ts, end_ts, label = parse_arg_to_window(sys.argv[1])
    raw_df = fetch_transactions(start_ts, end_ts, sample_limit=None)
    processed_df, _ = preprocess(raw_df)
    sample_size = max(1, int(0.10 * len(processed_df)))
    real_sample = processed_df.sample(n=sample_size, random_state=42) if sample_size < len(processed_df) else processed_df
    print(f"🧪 Sampled {len(real_sample)} records out of {len(processed_df)} real transactions")
    synthetic_count = max(1, int(0.10 * len(real_sample)))
    print(f"🧬 Generating {synthetic_count} synthetic records (10% of real sample)")
    synthetic_df = generate_synthetic_data(processed_df, synthetic_count=synthetic_count)
    model, run_id = train_isolation_forest(real_sample, synthetic_df)
    register_model_with_mlflow(run_id)
    print(f"Pipeline complete. Window={label}")
