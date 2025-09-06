# **SPLF Offline Backtest Engine (Binance USDⓈ-M)**

This project implements a fully offline backtesting engine for the "StormComing \+ Leader/State" (SPLF) trading model. It's designed for a research-oriented workflow using Python scripts and Jupyter Notebooks, allowing you to analyze and verify every step of the signal generation process.

As a trader familiar with tools like CryptoQuant, think of this as building your own custom, high-resolution indicator from the ground up.

## **Project Workflow**

The entire process is a pipeline that transforms raw, tick-level data into actionable performance metrics. Each stage produces artifacts that feed into the next.

graph TD  
    A\[\_1. Raw Data Ingestion\_\] \--\> B\[\_2. 1-Minute Bar Processing\_\];  
    B \--\> C\[\_3. Feature Engineering\_\];  
    C \--\> D\[\_4. Walk-Forward Backtest\_\];  
    D \--\> E\[\_5. Analysis & Reporting\_\];

    subgraph "splf/data\_handler"  
        A  
        B  
    end

    subgraph "splf/feature\_engine"  
        C  
    end

    subgraph "splf/modeling & splf/backtesting"  
        D  
    end

    subgraph "notebooks/"  
        E  
    end

    style A fill:\#f9f,stroke:\#333,stroke-width:2px  
    style B fill:\#f9f,stroke:\#333,stroke-width:2px  
    style C fill:\#ccf,stroke:\#333,stroke-width:2px  
    style D fill:\#fca,stroke:\#333,stroke-width:2px  
    style E fill:\#cfc,stroke:\#333,stroke-width:2px

**1\. Raw Data Ingestion (notebooks/01\_...):**

* **Input:** Symbol, date range.  
* **Process:** The BinanceDownloader script downloads compressed raw daily files (AggTrades, Klines, etc.) from Binance Vision.  
* **Output:** .zip files stored in data/raw/{SYMBOL}/.

**2\. 1-Minute Bar Processing (notebooks/01\_...):**

* **Input:** Raw .zip files.  
* **Process:** Scripts parse the raw data, decompressing, cleaning, and resampling everything onto a precise 1-minute time grid. This is a crucial step to align all different data sources.  
* **Output:** Cleaned, unified 1-minute bars saved as .parquet files in data/processed/{SYMBOL}/.

**3\. Feature Engineering (notebooks/02\_...):**

* **Input:** 1-minute bars (.parquet).  
* **Process:** The feature engine calculates all the specified indicators (CVD, basis, RV, etc.) on a rolling basis. The features are calculated at a 5-minute interval but are updated every 1 minute.  
* **Output:** A rich feature dataset, features\_5m.parquet, stored in data/features/{SYMBOL}/. This is the direct input for the model.

**4\. Walk-Forward Backtest (notebooks/03\_...):**

* **Input:** The features\_5m.parquet file.  
* **Process:** This is the core of the backtest. The BacktestRunner iterates through time, trains the Isolation Forest model on a rolling window of past data, and scores each new data point to detect anomalies ("Storms"). When a storm is confirmed, it's labeled with its state (e.g., "perp-led").  
* **Output:** An alerts.csv file containing every detected event, its timestamp, score, and labeled state.

**5\. Analysis & Reporting (notebooks/04\_...):**

* **Input:** The alerts.csv file and price data.  
* **Process:** This is where you, the researcher, take over. In this notebook, you'll attach outcome labels to each alert (e.g., "did the price explode by 3% in the next 60 minutes?"). You'll calculate performance metrics like Precision and Recall and visualize the results.  
* **Output:** A metrics.json file and various plots and charts that prove (or disprove) the model's effectiveness.

## **How to Use This Project**

Instead of running a single main.py file, the intended workflow is to step through the Jupyter Notebooks in the notebooks/ directory in numerical order. Start with 00\_Project\_Overview.ipynb.

1. **Setup:** Install the required packages: pip install \-r requirements.txt.  
2. **Navigate:** Open the notebooks/ directory.  
3. **Run:** Follow the notebooks from 00 to 04 to run the entire pipeline and analyze the results.