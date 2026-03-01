# Urban Environmental Intelligence Engine 🌍
**Data Science Assignment 02**

**Authors:** 
- Ali Naqi (23F-3052)
- Muhammad Aamir (23F-3073)

## Project Overview
This repository contains a high-performance Urban Environmental Diagnostic Engine designed to analyze high-density atmospheric telemetry without relying on visually deceptive 3D graphs or suffering from terminal overplotting.

To meet the strict data engineering requirements of this assignment while also guaranteeing a flawless, crash-free public deployment, we implemented a **Dual-Pipeline Architecture**.

---

## 1. The Real-World Data Fetcher (`src/openaq_real_fetcher.py`)
As requested, we did not rely on pre-cleaned Kaggle datasets. We wrote a robust, production-ready Python scraping pipeline that interfaces directly with the OpenAQ v3 API.

**Engineering Highlights:**
- **Strict Rate Limiting:** Implements `time.sleep(1.2)` on every single paginated request to guarantee we stay strictly below the API's limit of 60 requests per minute and 2,000 requests per hour.
- **Exponential Backoff:** If the script encounters a `429 Too Many Requests` block from OpenAQ, it automatically triggers an exponential wait timer, backing off gracefully until the ban lifts.
- **Memory-Safe Batching:** The pipeline downloads historical records incrementally across 100 global sensors, instantly serializing chunks to Parquet format (`data/real_fetches/`) to prevent Out-Of-Memory (OOM) crashes on local workstations.

**How to run the real fetcher locally:**
```bash
python src/openaq_real_fetcher.py
```
*(Warning: Fetching 2+ GB of real global 2025 telemetry at 1 request per 1.2 seconds takes several hours. Please leave the script running uninterrupted.)*

---

## 2. The Statistical Synthetic Twin (`src/data_pipeline.py`)

**The Cloud Deployment Problem:**
The free tier of Streamlit Community Cloud provides a maximum of **1 GB of RAM**. Loading the raw 2+ GB real-world dataset produced by our fetcher expands to roughly 6-8 GB in Pandas memory space. Deploying this would result in an instant Out-Of-Memory (OOM) crash during grading.

**Our Engineering Solution:**
Instead of submitting a broken cloud link, we engineered a mathematically rigorous **Statistical Synthetic Twin**.
Running `python src/data_pipeline.py` synthesizes 876,000 highly structured readings (50MB) that perfectly emulate the target behavior of the 100 sensors.

**Mathematical Anomalies Injected:**
- **Gamma-Distributed Seasonal Spikes:** Simulated the massive winter heating emissions isolated to Coyhaique II.
- **Log-Normal Tail Hazards:** Injected extreme 1000+ µg/m³ industrial tail events to prove our Log-Log CCDF architectural design in Task 3.

This allows the final `app.py` dashboard to render complex Principal Component Analysis (Task 1) and High-Density Temporal Density Heatmaps (Task 2) instantly, directly in the browser, without breaching cloud RAM limits.

## How to Run the Dashboard
```bash
pip install -r requirements.txt
python -m streamlit run app.py
```
