import os
import sys
import logging

from src.data_pipeline import simulate_openaq_fetch_and_geocoding

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def main():
    """
    Main entry point for the Urban Environmental Intelligence diagnostic pipeline.
    This script executes the 'Big Data Handling' fetching and preprocessing.
    The Interactive Dashboard is a separate Streamlit process.
    """
    logging.info("--- Phase 1: Global Node Discovery & Data Ingestion ---")
    
    # Run the simulated 2h 37m fetch and geocoding process
    # Generates data/simulated_env_2025.parquet
    simulate_openaq_fetch_and_geocoding()
    
    logging.info("--- Data Pipeline Execution Completed Successfully ---")
    logging.info("To view the interactive diagnostic engine, run:")
    logging.info("  streamlit run app.py")

if __name__ == "__main__":
    main()
