# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import IsolationForest
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import get_column_plot
from sdv.evaluation.single_table import evaluate_quality
from mlflow.tracking import MlflowClient
import mlflow
import mlflow.sklearn
import pandas as pd
from datetime import datetime, timedelta
import time
import os, sys
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import neo4j

# Point to the backend SQLite db for mlflow
mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("AML Isolation Forest Train")
# Ensure the artifact output folder exists
PLOT_DIR = "plots"
os.makedirs(PLOT_DIR, exist_ok=True)
# Ensure the report output folder exists
REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)
# Ensure the report output folder exists
METADATA_DIR = "metadata"
os.makedirs(METADATA_DIR, exist_ok=True)
# path for Spark/Neo4j connection lib
JAR_PATH = os.path.join(os.getcwd(), "lib/neo4j-connector-apache-spark_2.12-4.1.4_for_spark_3.jar")
# how many days back to look for training data
DEFAULT_DAYS_AGO = 7

# STEP 1: Data Access from Neo4j with Spark
def fetch_transactions(start_ts, end_ts, sample_limit):
    readable_start = datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M:%S')
    readable_end = datetime.fromtimestamp(end_ts).strftime('%Y-%m-%d %H:%M:%S')
    print(f"Fetching transactions between {readable_start} and {readable_end}...")
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
            //ORDER BY rand()
            //LIMIT {sample_limit}
        }}
        RETURN *
    """
    # Initialize Spark Session
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("AML Train Pipeline Node") \
        .config("spark.jars", JAR_PATH) \
        .getOrCreate()
    # "/root/tese_henrique/aml_pipeline/lib/neo4j-connector-apache-spark_2.12-4.1.4_for_spark_3.jar"
    print("Spark Session initialized.")

    # spark.neo4j.
    df = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://192.168.0.5:7687") \
        .option("authentication.type", "basic") \
        .option("authentication.basic.username", "neo4j") \
        .option("authentication.basic.password", "PotatoDTND12!") \
        .option("query", query) \
        .load()
    print(f"Transactions fetched: {df.count()} records.")
    return df

# STEP 2: Pre-processing & Feature Engineering
def preprocess(df):
    print("Starting pre-processing...")
    pdf = df.toPandas()

    # Scaling not necessary but need to be consistent on scoring
    #scaler = MinMaxScaler()
    #scaled_features = scaler.fit_transform(pdf[[
    #    'amount', 'from_pagerank', 'to_pagerank', 'from_centrality', 'to_centrality'
    #]])

    #scaled_df = pd.DataFrame(scaled_features, columns=[
    #    'amount', 'from_pagerank', 'to_pagerank', 'from_centrality', 'to_centrality'
    #])

    #scaled_df['transaction_id'] = pdf['transaction_id'].values
    scaled_df = pdf[[
    'transaction_id', 'amount', 'from_pagerank', 'to_pagerank', 'from_centrality', 'to_centrality']].copy()
    print("Pre-processing completed.")
    return scaled_df, None #scaler

# STEP 3: Synthetic Data Generation with GAN 
def generate_synthetic_data(df, synthetic_count):
    
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 0)
    pd.set_option("display.max_colwidth", None)
    
    print(f"Generating {synthetic_count} synthetic data samples using SDV CTGANSynthesizer...")

    # Remove transaction_id before fitting
    # Incoming sampled data
    #print(df)
    train_df = df.drop(columns=['transaction_id'])
    
    # Define metadata
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(data=train_df)

    # Train SDV CTGAN
    #model = CTGANSynthesizer(metadata)
    model = CTGANSynthesizer(
        metadata,
        epochs=50,                    # fewer epochs
        verbose=True                 # show training progress
    )
    # print column names before train to check if they are available
    # print(train_df.columns.values)
    model.fit(train_df)

    # Generate synthetic samples
    synthetic_scaled = model.sample(synthetic_count)

    # Assign sampled transaction IDs to synthetic data
    synthetic_data = pd.DataFrame(
        df['transaction_id'].sample(n=synthetic_count, replace=True).values,
        columns=['transaction_id']
    )
    synthetic_data = pd.concat([synthetic_data.reset_index(drop=True), synthetic_scaled.reset_index(drop=True)], axis=1)

    print("Synthetic data generation completed using SDV.")
    
    # print(synthetic_data)

    return synthetic_data

# STEP 4: Isolation Forest Training & Validation
def train_isolation_forest(real_data, synthetic_data):
    with mlflow.start_run() as run:
        start_time = time.time()

        combined_data = pd.concat([real_data, synthetic_data])
        features = combined_data.drop(columns=['transaction_id'])
        isolation_forest = IsolationForest(contamination='auto', random_state=42)
            
        #pd.set_option("display.max_rows", None)
        #pd.set_option("display.max_columns", None)
        #pd.set_option("display.width", 0)
        #pd.set_option("display.max_colwidth", None)
        #print(features)
        
        isolation_forest.fit(features)

        # Metrics and params
        train_time = time.time() - start_time
        mlflow.log_param("synthetic_sample_size", len(synthetic_data))
        mlflow.log_param("real_sample_size", len(real_data))
        mlflow.log_metric("training_time_sec", train_time)
        mlflow.log_metric("synthetic_to_real_ratio", len(synthetic_data) / len(real_data))
        mlflow.log_dict({"feature_order": features.columns.tolist()}, "feature_order.json")

        input_example = features.iloc[:5]
        mlflow.sklearn.log_model(isolation_forest, "isolation_forest_model", input_example=input_example)

        # Save SDV plots, metadata and report and log as artifacts
        log_sdv_plots_report_metadata(real_data.drop(columns=['transaction_id']),
                      synthetic_data.drop(columns=['transaction_id']))

        print("✅ MLflow logging complete with SDV plots.")
    return isolation_forest, run.info.run_id

# EXTRA: plot real and synthetic data for comparison
# log quality metrics for generated vs real data
def log_sdv_plots_report_metadata(real_df, synthetic_df):
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(real_df)

    os.makedirs(PLOT_DIR, exist_ok=True)
    os.makedirs(REPORT_DIR, exist_ok=True)

    # print on screen & save on MLFlow a quality report
    quality_report = evaluate_quality(real_df, synthetic_df, metadata)
    report_path = os.path.join(REPORT_DIR, "quality_report.pkl")
    quality_report.save(filepath=report_path)
    mlflow.log_artifact(report_path)

    # log the quality metrics    
    mlflow.log_metric("sdv_overall_quality_score", quality_report.get_score())
    scores = quality_report.get_properties()
    score_dict = dict(zip(scores["Property"], scores["Score"]))
    mlflow.log_metric("sdv_column_shapes_score", score_dict["Column Shapes"])
    mlflow.log_metric("sdv_column_pair_trends_score", score_dict["Column Pair Trends"])

    # save metadata for reproducibility
    metadata_path = os.path.join(METADATA_DIR, "sdv_metadata.json")
    if os.path.exists(metadata_path):
        os.remove(metadata_path)
    metadata.save_to_json(metadata_path)
    mlflow.log_artifact(metadata_path)

    for column in real_df.columns:
        if column == 'transaction_id':
            continue
        try:
            plot = get_column_plot(
                real_data=real_df,
                synthetic_data=synthetic_df,
                metadata=metadata,
                column_name=column
            )

            if isinstance(plot, go.Figure):
                plot.update_layout(title_text=f"Distribution Comparison - {column}")
                plot_path = os.path.join(PLOT_DIR, f"{column}_dist_comparison.png")
                plot.write_image(plot_path)
                mlflow.log_artifact(plot_path)
            else:
                print(f"⚠️ Unknown plot object type for {column}. Skipping save.")

        except Exception as e:
            print(f"Skipping plot for column {column} due to error: {e}")

def register_model_with_mlflow(run_id, model_name="isolation_forest_model"):
    result = mlflow.register_model(
        model_uri=f"runs:/{run_id}/{model_name}", # model
        name=model_name
    )

    client = MlflowClient()
    client.transition_model_version_stage(
        name=model_name,
        version=result.version,
        stage="Production",
        archive_existing_versions=True
    )

    print(f"✅ Model registered and promoted to Production: {model_name} v{result.version}")

def get_training_window(days_ago):
    print("🔍 Looking up latest enriched transaction timestamp...")
    driver = neo4j.GraphDatabase.driver("bolt://192.168.0.5:7687", auth=("neo4j", "PotatoDTND12!"))
    with driver.session() as session:
        result = session.run("""
            MATCH (tx:Transaction)
            WHERE tx.enriched_at IS NOT NULL
            RETURN max(tx.timestamp) AS latest_ts
        """)
        row = result.single()
        end_timestamp = int(row["latest_ts"])
        start_timestamp = int(end_timestamp - 86400 * days_ago)
        print(f"⏱ Training window: {start_timestamp} to {end_timestamp} (from {days_ago} days ago)")
        return start_timestamp, end_timestamp

# Main Execution (dynamic timestamps)
if __name__ == "__main__":
    try:
        days_ago = int(sys.argv[1])
    except (IndexError, ValueError):
        days_ago = DEFAULT_DAYS_AGO

    # Determine end time from latest enriched transaction
    start_timestamp, end_timestamp = get_training_window(days_ago)

    # Fetch Data
    raw_df = fetch_transactions(start_timestamp, end_timestamp, sample_limit=None)

    # Pre-process Data
    processed_df, scaler = preprocess(raw_df)

    # Sample 10% of real data
    sample_size = max(1, int(0.10 * len(processed_df)))
    real_sample = processed_df.sample(n=sample_size, random_state=42)
    print(f"🧪 Sampled {sample_size} records out of {len(processed_df)} real transactions (10%)")

    # Generate Synthetic Data: 10% of sampled real data
    synthetic_count = max(1, int(0.10 * len(real_sample)))
    print(f"🧬 Generating {synthetic_count} synthetic records (10% of sample)")
    synthetic_df = generate_synthetic_data(processed_df, synthetic_count=synthetic_count)

    # Train Isolation Forest
    model, run_id = train_isolation_forest(real_sample, synthetic_df)
    register_model_with_mlflow(run_id)

    print("Pipeline execution complete.")