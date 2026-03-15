# Urban Environmental Intelligence Engine 🌍
**Data Science Assignment 02**

**Authors:** 
- Ali Naqi (23F-3052)
- Muhammad Aamir (23F-3073)

## Project Overview
This repository contains a high-performance Urban Environmental Diagnostic Engine designed to analyze high-density atmospheric telemetry without relying on visually deceptive 3D graphs or suffering from terminal overplotting.

**IMPORTANT:** This project uses **100% Real-World Global Telemetry**. We have successfully engineered a high-performance **Parquet-based Binary Pipeline** that allows several gigabytes of authentic environmental data to run instantly in the browser without synthetic simulation.

---

## 1. The Real-World Data Pipeline (`src/fetch_openaq_clean.py`)
We did not rely on pre-cleaned or static Kaggle datasets. We developed a production-ready ingestion pipeline that interfaces directly with the **OpenAQ v3 API** and the **Open-Meteo Air Quality** services.

### Engineering Highlights:
- **Massive Scale Ingestion:** Fetches accurate hourly historical data for the entire year of 2025 across 100 distinct global sensor stations.
- **Strict Rate Limiting:** Implements paginated request throttling to comply with international API standards.
- **Columnar Binary Storage (Parquet):** We solved the "Cloud Memory Problem" by serializing over 2GB of raw text data into **Parquet format**. This columnar compression allows the 876,000+ reading database to load instantly into the Streamlit UI (~11MB) with zero data loss.
- **Automated Data Cleaning:** The pipeline handles real-world hardware failures by performing automated NaN imputation and unit standardization across globally disparate sources.

---

## 2. Advanced Analytical Tasks

### Task 1: Dimensionality Reduction (PCA)
- **Problem:** Analyzing 6 interrelated atmospheric variables across 100 nodes.
- **Solution:** Principal Component Analysis (PCA) to compress the state space into 2D clusters.
- **Result:** Successfully identified the primary drivers of urban pollution and clustered **Industrial vs. Residential** zones based on PC1 loading variance.

### Task 2: High-Density Temporal Analysis
- **Solution:** Implemented temporal density heatmaps to identify the **"Periodic Signature"** of global pollution events across a full calendar year.

### Task 3: Distribution & Tail Integrity
- **Solution:** Utilized Log-Log CCDF (Complementary Cumulative Distribution Function) plots to reveal the "Long Tail" hazards that standard histograms often obscure.

### Task 4: Visual Integrity Audit
- **Solution:** Rejected 3D charts due to high **Lie Factor**. Implemented Tufte-compliant **Small Multiples** with a perceptually uniform **Viridis** color scale to ensure scientific honesty.

---

## How to Run the Dashboard
1. **Clone the Repo**
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Launch the Engine:**
   ```bash
   python -m streamlit run app.py
   ```

© 2026 Developed for the Urban Environmental Intelligence Platform.
