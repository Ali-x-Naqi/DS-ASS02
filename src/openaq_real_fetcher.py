import os
import time
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta

# Set up professional logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler("openaq_fetch.log")]
)

class OpenAQRealFetcher:
    """
    Robust fetcher for OpenAQ API v3. 
    Strictly designed to handle the 60 requests/minute and 2000 requests/hour rate limits,
    including exponential backoff for 429 Too Many Requests errors.
    """
    BASE_URL = "https://api.openaq.org/v3"
    
    def __init__(self, api_key=None):
        self.headers = {"X-API-Key": api_key} if api_key else {}
        self.output_dir = "data/real_fetches"
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _safe_request(self, endpoint, params=None):
        """
        Executes a GET request with strict rate limiting and 429 Error handling.
        """
        url = f"{self.BASE_URL}/{endpoint}"
        max_retries = 5
        base_wait = 60 # seconds to wait if we hit a 429

        for attempt in range(max_retries):
            try:
                # STRICT RATE LIMITING: Guaranteed <60 req/min
                time.sleep(1.2) 
                
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    wait_time = base_wait * (2 ** attempt)
                    logging.warning(f"429 Too Many Requests hit on {endpoint}. Backing off for {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"API Error {response.status_code} on {endpoint}: {response.text}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                logging.error(f"Network error on attempt {attempt+1}: {e}")
                time.sleep(10)
                
        logging.error(f"Failed to fetch {endpoint} after {max_retries} retries.")
        return None

    def fetch_locations(self, limit=100):
        """Fetches active monitoring locations."""
        logging.info(f"Fetching {limit} locations...")
        data = self._safe_request("locations", {"limit": limit, "has_geo": "true"})
        if not data or "results" not in data:
            return []
        
        locations = [{"id": loc['id'], "name": loc['name'], "country": loc['country']['code']} 
                     for loc in data['results']]
        logging.info(f"Successfully retrieved {len(locations)} locations.")
        return locations

    def download_measurements(self, location_id, date_from, date_to):
        """
        Downloads measurements for a specific location using pagination
        to ensure we capture high-density temporal data.
        """
        all_measurements = []
        page = 1
        
        while True:
            params = {
                "date_from": date_from,
                "date_to": date_to,
                "limit": 1000,
                "page": page
            }
            data = self._safe_request(f"locations/{location_id}/measurements", params)
            
            if not data or "results" not in data or len(data["results"]) == 0:
                break
                
            for res in data["results"]:
                all_measurements.append({
                    "location_id": location_id,
                    "parameter": res["parameter"]["name"],
                    "value": res["value"],
                    "unit": res["unit"],
                    "timestamp": res["period"]["datetimeFrom"]["utc"]
                })
                
            logging.info(f"   - Location {location_id}: Fetched {len(data['results'])} records (Page {page})")
            
            if len(data["results"]) < 1000:
                break # Last page reached
            page += 1
            
        return all_measurements

    def run_batch_pipeline(self, num_locations=20, days_back=30):
        """
        Orchestrates the massive data pull, saving incrementally to prevent data loss 
        during long-running API heavy jobs.
        """
        date_to = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        date_from = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        logging.info(f"Initiating Batch Pipeline: Fetching {days_back} days of data for {num_locations} locations.")
        locations = self.fetch_locations(limit=num_locations)
        
        master_data = []
        for loc in locations:
            logging.info(f"Processing Location: {loc['name']} ({loc['country']})")
            measurements = self.download_measurements(loc['id'], date_from, date_to)
            
            if measurements:
                df = pd.DataFrame(measurements)
                # Save checkpoint immediately to protect against crash/ban
                filename = f"{self.output_dir}/loc_{loc['id']}_data.parquet"
                df.to_parquet(filename, index=False)
                master_data.append(df)
                
        if master_data:
            final_df = pd.concat(master_data, ignore_index=True)
            final_file = f"{self.output_dir}/MASTER_DATASET_{datetime.utcnow().strftime('%Y%m%d')}.parquet"
            final_df.to_parquet(final_file, index=False)
            logging.info(f"Pipeline Complete! Master dataset saved to {final_file} with {len(final_df)} total records.")
        else:
            logging.warning("Pipeline finished but no data was collected.")

if __name__ == "__main__":
    # Initialize the fetcher script. Provide an API key if you have authenticated with OpenAQ.
    # To run this for the full assignment requirement, increase num_locations to 100.
    fetcher = OpenAQRealFetcher(api_key=None) 
    fetcher.run_batch_pipeline(num_locations=10, days_back=30)
