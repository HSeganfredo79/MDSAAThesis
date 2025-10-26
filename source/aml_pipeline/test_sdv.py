from sdv.single_table import CTGANSynthesizer
from sklearn.preprocessing import MinMaxScaler
import pandas as pd

# Create the initial DataFrame
df = pd.DataFrame({
    'amount': [100, 2000, 30000, 4000000],
    'from_pagerank': [0.00001, 0.2, 0.5, 0.99999],
    'to_pagerank': [0.00001, 0.4, 0.7, 0.99999],
    'from_centrality': [0, 0.3, 0.799999, 1],
    'to_centrality': [0, 0.499999, 0.6, 1]
})
#Metric	Min Value	Max Value	Notes
#pagerank(G)	> 0	< 1	Values sum to 1 across all nodes
#degree_centrality(G)	0	1	Max = fully connected node; min = isolated node

# Optional: Scale numeric data
scaler = MinMaxScaler()
scaled_data = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)

# Define metadata explicitly (recommended for SDV 1.19+)
from sdv.metadata import SingleTableMetadata

metadata = SingleTableMetadata()
metadata.detect_from_dataframe(data=scaled_data)

# Train CTGANSynthesizer
model = CTGANSynthesizer(metadata)
model.fit(scaled_data)

# Sample synthetic data
synthetic_scaled = model.sample(500)

# Inverse transform to original scale
synthetic_data = pd.DataFrame(
    scaler.inverse_transform(synthetic_scaled),
    columns=df.columns
)

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 0)
pd.set_option("display.max_colwidth", None)

print("✅ Synthetic data generated successfully:")
print(synthetic_data)
