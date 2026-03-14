# Smart City Diagnostic Engine: Viva Preparation Guide

## 1. Overall Purpose of the Assignment
The primary objective of this assignment was to build a robust **Smart City Diagnostic Engine** capable of ingesting, analyzing, and visualizing large-scale environmental telemetry data. We were tasked with analyzing six different pollution metrics (PM2.5, PM10, O3, NO2, SO2, CO) monitored across 100 sensor stations situated in various city zones (Industrial vs. Residential). 

Instead of generating synthetic artificial data, we fundamentally elevated the project by connecting directly to real-world APIs (**OpenAQ** and **Open-Meteo**) to retrieve authentic global telemetry for the year 2025. The ultimate goal was to identify hidden patterns through advanced statistical methods—dimensionality reduction, temporal tracking, extreme value modeling, and strict visual integrity audits.

## 2. Why We Did What We Did
- **Real Data Migration:** We chose to extract real-world data from OpenAQ. Real data has missing values, skewed distributions, and erratic spikes. Solving these challenges proves real-world engineering capability to the grader.
- **Architectural Refactoring:** We modularized the codebase, separating data fetching (`src/fetch_openaq_clean.py`), data transforming, and the actual visualization (`dashboard/app.py`). This prevents "spaghetti code" and allows massive datasets to load instantly via cache.
- **Advanced UI Overhaul:** We overhauled the Streamlit dashboard using a very modern, custom CSS *Glassmorphism* design. The goal was to make the project visually distinct, professional, and vastly superior to a standard out-of-the-box Streamlit template.

## 3. Core Libraries & APIs Used
- **Pandas:** For high-performance data manipulation, filtering, handling missing values (NaN imputation), and joining spatial-temporal data.
- **NumPy:** For complex mathematical operations, calculating variance, covariance matrices, and percentile bounds.
- **Scikit-Learn (PCA, StandardScaler):** Used exclusively for Task 1 to standardize units and execute the Principal Component Analysis mathematical transformations.
- **Plotly (Express & Graph Objects):** Chosen over Matplotlib because it generates fully interactive WebGL-rendered charts capable of displaying hundreds of thousands of data points without crashing the browser.
- **Requests & JSON:** Used to execute HTTP requests to the REST APIs and handle the structural JSON payloads.
- **Streamlit:** Our core frontend framework, wrapping the Python logic into an interactive web application.

## 4. The OpenAQ Data Extraction (The 5-Hour Process)
**Why did it take 5 hours?**
Retrieving a full year (8,760 hours) of continuous, granual data across 100 distinct locations requires sending hundreds of thousands of requests to the OpenAQ servers. 

- **The API Key:** OpenAQ restricts anonymous users to very low rate limits. We registered for an API Key, which we passed securely in the HTTP Headers (`"X-API-Key"`). This unlocked higher bandwidth.
- **Pagination & Throttling:** Even with a key, a server will reject you if you ask for 1,000,000 rows at once. Our extraction script elegantly "paginated" through the data month-by-month, station-by-station.
- **Robustness:** A 5-hour script over the internet *will* drop connection eventually. We engineered the script with automatic retries, back-off timers, and checkpointing so that if it disconnected at hour 4, it wouldn't start over from scratch.

## 5. Why is the Dataset File Size So Small?
When downloading JSON strings from the API, the payload is multiple gigabytes because JSON repetitively sends massive text strings like `"parameter": "pm25", "value": 15.3` for every single row.

After fetching, we loaded this massive JSON framework into Pandas and immediately saved it to disk as a **`.parquet`** file via the PyArrow engine. 
- **Columnar Binary Storage:** Parquet is a columnar binary format. It groups identical data types together rather than row-by-row.
- **High Compression (Snappy):** Because all values in a column are just floats (e.g., 15.3, 14.1, 16.2), the format applies extreme run-length encoding and compression. A gigabyte of text is crushed down to just a few megabytes of binary data, with zero data loss. It is the enterprise standard for Big Data.

---

## 6. Detailed Task Breakdown

### Task 1: The Dimensionality Challenge (PCA)
- **The Problem:** We had 6 air quality variables. Graphing 6 dimensions simultaneously is mathematically impossible on a standard 2D monitor, resulting in chaotic visual overlap.
- **The Solution:** We used **Principal Component Analysis (PCA)**. First, we ran `.fit_transform()` via a `StandardScaler` to ensure metrics with different units (ppm vs µg/m³) didn't dominate the math.
- **The Result:** We compressed 6 dimensions into a 2D space (PC1 vs PC2) conserving most of the mathematical variance. PC1 captures combustion/industrial emissions. A scatter plot of PC1 instantly clusters Industrial zones cleanly away from Residential zones.

### Task 2: High-Density Temporal Analysis
- **The Problem:** 100 stations tracked over an entire year causes extreme "spaghetti plot" clutter if drawn as 100 overlapping line charts.
- **The Solution:** We implemented a High-Density Temporal Heatmap spanning the months on the X-axis and 100 stations on the Y-axis. Colors encoded **"Health Threshold Violations"** (PM2.5 > 35 µg/m³).
- **The Analysis:** The visualization mathematically proved a **"Periodic Signature"** tied to macro-seasonal shifts (like winter weather trapping smog or summer wildfires), ruling out standard daily traffic commutes as the main driver of massive global hazard spikes.

### Task 3: Distribution Modeling & Tail Integrity
- **The Problem:** Standard histograms aggregate data into "bins". When dealing with extreme pollution events (the rare spikes), these bins compress the high values into invisible visual "noise" on the far right of the graph.
- **The Solution:** We generated two distinct statistical geometries:
  1. A standard Linear KDE (Kernel Density Estimate) to show the *Peak* average.
  2. A **Log-Log CCDF** (Complementary Cumulative Distribution Function).
- **The Analysis:** The Log-Log scale effectively "zooms out" the X and Y axes, proving that the extremely rare **"Long Tail"** events (> 200 µg/m³) occur consistently enough to require intervention. It offers a mathematically "honest" look at the 99th percentile hazard zone.

### Task 4: The Visual Integrity Audit
- **The Problem:** We were proposed a 3D bar chart to map Pollution vs Population Density vs Region.
- **The Audit Verdict:** We definitively **rejected** the 3D bar chart. 3D charts introduce severe volumetric distortion (a high *Lie Factor*), tricking the human eye because closer 3D bars look vastly disproportionately larger than bars physically placed "behind" them in the Z-plane.
- **The Fix:** We implemented **Tufte-Compliant Small Multiples**. By aligning multiple independent 2D scatter plots horizontally on identical scales, we vastly improved the **Data-Ink Ratio**.
- **Color Scale Justification:** We actively rejected the 'Rainbow' color mapping due to non-linear luminance jumps (which cause false boundaries to appear in data). Instead, we used **"Viridis" (Sequential)**, because a sequential scale increases in visual luminance monotonically, natively matching how the human retina perceives rising intensity.
