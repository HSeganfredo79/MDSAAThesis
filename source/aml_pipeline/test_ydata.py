from ydata_synthetic.synthesizers.regular import RegularSynthesizer
from ydata_synthetic.synthesizers import ModelParameters, TrainParameters
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

# === Step 0: Data Generator with boundaries ===
def generate_training_data(n_rows=100, seed=42):
    np.random.seed(seed)

    # amount: ≥ 0, long-tail
    amount = np.random.lognormal(mean=7, sigma=1.0, size=n_rows)
    amount = np.clip(amount, 0, None)

    # pagerank: (0, 1)
    from_pagerank = np.clip(np.random.beta(a=2, b=5, size=n_rows), 0.0001, 0.9999)
    to_pagerank = np.clip(np.random.beta(a=3, b=3, size=n_rows), 0.0001, 0.9999)

    # centrality: [0, 1]
    from_centrality = np.random.uniform(0, 1, size=n_rows)
    to_centrality = np.random.uniform(0, 1, size=n_rows)

    df = pd.DataFrame({
        'amount': amount,
        'from_pagerank': from_pagerank,
        'to_pagerank': to_pagerank,
        'from_centrality': from_centrality,
        'to_centrality': to_centrality
    })

    return df

# === Step 1: Generate 100 clean + diverse samples
df = generate_training_data(n_rows=100)

# === Step 2: Preprocessing
num_cols = df.columns.tolist()
cat_cols = []

scaler = MinMaxScaler()
scaled_data = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)

# === Step 3: CTGAN training
ctgan_args = ModelParameters(batch_size=500, lr=2e-4, betas=(0.5, 0.9))
train_args = TrainParameters(epochs=100)

synth = RegularSynthesizer(modelname='ctgan', model_parameters=ctgan_args)
synth.fit(data=scaled_data, train_arguments=train_args, num_cols=num_cols, cat_cols=cat_cols)

# === Step 4: Generate synthetic samples
synthetic_data = synth.sample(10)
synthetic_df = pd.DataFrame(scaler.inverse_transform(synthetic_data), columns=df.columns)

# === Step 5: Clip results back to enforce hard boundaries
synthetic_df['amount'] = synthetic_df['amount'].clip(lower=0)
synthetic_df['from_pagerank'] = synthetic_df['from_pagerank'].clip(lower=0.0001, upper=0.9999)
synthetic_df['to_pagerank'] = synthetic_df['to_pagerank'].clip(lower=0.0001, upper=0.9999)
synthetic_df['from_centrality'] = synthetic_df['from_centrality'].clip(0.0, 1.0)
synthetic_df['to_centrality'] = synthetic_df['to_centrality'].clip(0.0, 1.0)

# === Done
print("✅ CTGAN synthetic data (valid ranges):")
print(synthetic_df.head())
