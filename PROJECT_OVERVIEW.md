# 🏙️ Urban Environmental Intelligence Engine: Comprehensive Project Overview

## 1. Project Purpose & Objective
This project is a high-performance **Diagnostic Engine** designed to handle Big Data challenges in urban environmental monitoring. It was built for **Assignment 02** to analyze high-density telemetry from 100 sensors across Industrial and Residential zones.

The key breakthrough of this implementation is the migration from synthetic simulations to **Real-World Global Telemetry** via the OpenAQ and Open-Meteo APIs, covering a full year (2025) of atmospheric data.

---

## 2. System Architecture & File Directory
Below is a map of the production-ready files in the repository:

### Root Level
- **`app.py`**: The main entry point shim. It provides the execution context for Streamlit Cloud deployment.
- **`requirements.txt`**: Defined dependencies (Pandas, Scikit-Learn, Plotly, Streamlit, etc.).
- **`viva_prep.pdf` & `viva_prep.md`**: Comprehensive exam preparation materials.
- **`README.md`**: Initial project greeting and basic run instructions.

### `dashboard/` (The Frontend)
- **`app.py`**: The core Streamlit application logic. Handles the Glassmorphism UI, tab routing, and interactive visualization rendering.

### `src/` (The Intelligence Layer)
- **`fetch_openaq_clean.py` & `fetch_openmeteo_clean.py`**: The production data extraction scripts. They handle API rate-limiting, pagination, and data cleaning.
- **`task1_pca.py`**: Executes the mathematical Principal Component Analysis (Standardization -> Covariance -> Dimensionality Reduction).
- **`task2_temporal.py`**: Analyzes 876,000+ readings to generate high-density violation heatmaps and line plots.
- **`task3_distribution.py`**: Implements advanced statistical modeling (KDE vs Log-Log CCDF) to identify extreme hazard tails.
- **`task4_audit.py`**: Performs visual integrity transformations to maximize Data-Ink ratios (rejected 3D for Small Multiples).

### `data/` (The Database)
- **`openaq_dataset/`**: Contains 100+ individual station `.parquet` files and the aggregated 2025 master database.
- **`openmeteo_dataset/`**: Contains the satellite-corrected comparison database.

---

## 3. The Data Pipeline (ETL)
Our pipeline follows a strict **Extract-Transform-Load** cycle:

1.  **Extraction (5-Hour Cycle):** We reached out to the OpenAQ global node network using an API Key. We hit rate-limits and successfully negotiated the traffic via monthly pagination.
2.  **Transformation (Transformation & Cleaning):**
    *   **Unit Standardization:** Converted all units to a common scale.
    *   **Imputation:** Handled hardware-failure gaps (NaNs) in Ground Sensors using statistical mean-fill within station groups.
3.  **Load (Parquet Binary Serialization):** To handle Big Data in a small space, we converted raw JSON payloads into **Parquet**. This columnar binary format compressed over 1GB of text data into ~11MB with 100% data integrity.

---

## 4. Analytical Results & Methodology

### Dimensionality Reduction (Task 1)
- **Method:** Principal Component Analysis (PCA).
- **Result:** We successfully proved that **PM2.5 and PM10** contribute most to the PC1 axis, acting as the primary classifiers that separate Industrial zones from Residential baselines.

### Temporal Signature (Task 2)
- **Method:** High-Density Heatmap violation tracking (>35 µg/m³).
- **Result:** Found that pollution events are **Macro-Seasonal**. Violations occur simultaneously across the entire grid during specific months (inversions/seasons), proving they are not driven by daily traffic commutes alone.

### Tail Integrity & Extreme Hazards (Task 3)
- **Method:** Log-Log Complementary Cumulative Distribution Function (CCDF).
- **Result:** The CCDF revealed that extreme hazard events (> 200 µg/m³) are far more probable than a standard linear histogram suggests, identifying localized pollution spikes that are normally hidden.

### Visual Integrity Audit (Task 4)
- **Method:** Tufte Integrity Audit (Lie Factor & Data-Ink).
- **Result:** Rejected 3D charts for 2D **Small Multiples**. Replaced 'Rainbow' color schemes with the **Viridis** perceptually uniform scale to ensure accurate luminance perception by the user.

---

## 5. How to Run
1.  **Clone the Repo**
2.  **Install dependencies:** `pip install -r requirements.txt`
3.  **Launch Dashboard:** `streamlit run app.py`

© 2026 Developed with precision for maximum assignment impact.
