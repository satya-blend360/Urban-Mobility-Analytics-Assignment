import pandas as pd

# Load CSV
df = pd.read_csv("cleaned_taxi_data.csv")

# Randomly sample 10,000 rows
sampled_df = df.sample(n=10000, random_state=42)

# Save to new CSV
sampled_df.to_csv("cleaned_taxi_data_10k.csv", index=False)

print("Saved cleaned_taxi_data_10k.csv with 10,000 rows")
