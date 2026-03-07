import pandas as pd
from src.openmeteo_fetcher import fetch_city_data

# The 100th city!
extra_city = ("Reykjavik", "IS", 64.1466, -21.9426, "Residential")

df_existing = pd.read_parquet("data/processed_real_2025.parquet")
current_count = df_existing['station_name'].nunique()
print(f"Current count: {current_count}")

if current_count < 100:
    new_df = fetch_city_data(100, extra_city)
    if new_df is not None:
        df_combined = pd.concat([df_existing, new_df], ignore_index=True)
        df_combined.to_parquet("data/processed_real_2025.parquet", index=False)
        print(f"Appended 1 city. Total now: {df_combined['station_name'].nunique()}")
