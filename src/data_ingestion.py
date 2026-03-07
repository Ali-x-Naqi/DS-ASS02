import os
import glob
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def process_real_data(input_dir="data/real_fetches", output_path="data/processed_real_2025.parquet"):
    """
    Ingests all real-world Parquet files, pivots parameters, and saves a master dataset.
    """
    logging.info(f"Starting Ingestion from {input_dir}...")
    
    all_files = glob.glob(os.path.join(input_dir, "*.parquet"))
    if not all_files:
        logging.error("No Parquet files found in the input directory!")
        return

    logging.info(f"Found {len(all_files)} files. Loading...")
    
    dfs = []
    for f in all_files:
        try:
            temp_df = pd.read_parquet(f)
            dfs.append(temp_df)
        except Exception as e:
            logging.error(f"Error reading {f}: {e}")

    if not dfs:
        logging.error("No data loaded!")
        return

    df_master = pd.concat(dfs, ignore_index=True)
    logging.info(f"Combined Data: {len(df_master)} raw measurement rows.")

    # 1. Clean Timestamps
    df_master['timestamp'] = pd.to_datetime(df_master['timestamp'])

    # 2. Optimized Pivot
    logging.info("Pivoting data to wide format (Optimized GroupBy + Unstack)...")
    
    # We first ensure we have unique (timestamp, location_id, parameter)
    # Using groupby + mean + unstack is significantly faster than pivot_table for Big Data
    df_pivoted = df_master.groupby(["timestamp", "location_id", "location_name", "parameter"])["value"].mean().unstack("parameter").reset_index()
    
    # Clean up memory
    del df_master
    import gc
    gc.collect()

    # 3. Handle Metadata Enrichments (Assignment Specific) - Vectorized
    logging.info("Applying Vectorized Urban Zone and Country classification...")
    name_ser = df_pivoted['location_name'].str.lower()
    
    # Default tags
    df_pivoted['zone'] = "Residential"
    df_pivoted['country'] = "US"
    
    # Industrial Vectorized Tagging
    import re
    ind_keys = ["industrial", "power", "factory", "port", "zone", "highway", "traffic", "dpcc", "kerbside", "roadside"]
    ind_pattern = '|'.join([re.escape(k) for k in ind_keys])
    ind_mask = name_ser.str.contains(ind_pattern, na=False)
    df_pivoted.loc[ind_mask, 'zone'] = "Industrial"
    
    # Country Vectorized Tagging
    def safe_contains(pat): return name_ser.str.contains(pat, na=False)
    
    df_pivoted.loc[safe_contains("delhi|dpcc"), 'country'] = "IN"
    df_pivoted.loc[safe_contains("chile|concep|valp|puente|tala|quili"), 'country'] = "CL"
    df_pivoted.loc[safe_contains("london|haringey|marylebone|westminster"), 'country'] = "GB"
    df_pivoted.loc[safe_contains("groningen|rotterdam|utrecht|heerlen|zaanstad"), 'country'] = "NL"
    df_pivoted.loc[safe_contains("berlin|frankfurt|munich"), 'country'] = "DE"

    # Normalize terminology
    df_pivoted = df_pivoted.rename(columns={
        "location_id": "station_id",
        "location_name": "station_name",
        "pm25": "pm25",
        "pm10": "pm10",
        "no2": "no2",
        "o3": "o3",
        "temperature": "temp",
        "relativehumidity": "humidity"
    })
    
    # Cleanup station names - Use literal split
    df_pivoted['station_name'] = df_pivoted['station_name'].astype(str).str.split(" (ID:", regex=False).str[0]

    # 4. Fill missing required columns
    req_cols = ["pm25", "pm10", "no2", "o3", "temp", "humidity"]
    for col in req_cols:
        if col not in df_pivoted.columns:
            df_pivoted[col] = pd.NA

    # 5. Add simulated population density
    import numpy as np
    np.random.seed(42)
    stations = df_pivoted['station_id'].unique()
    station_pop = {s: np.random.randint(1000, 25000) for s in stations}
    df_pivoted['pop_density'] = df_pivoted['station_id'].map(station_pop)

    # 6. Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_pivoted.to_parquet(output_path, index=False)
    
    logging.info(f"Ingestion Complete. Saved {len(df_pivoted)} records to {output_path}")
    logging.info(f"Final Count - Industrial: {len(df_pivoted[df_pivoted['zone']=='Industrial'])} | Residential: {len(df_pivoted[df_pivoted['zone']=='Residential'])}")

if __name__ == "__main__":
    process_real_data()
