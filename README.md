# EMA Crossover Analysis for BTC-USD & Power BI Dashboard

## Project Overview

This project provides an automated data pipeline and an interactive Power BI dashboard for analyzing Exponential Moving Average (EMA) crossovers for BTC-USD. The goal is to provide timely insights into potential bullish and bearish market signals, helping traders understand price momentum and historical crossover patterns.

The system automates the following:

1.  **Data Collection**: Fetches historical **daily candle** BTC-USD OHLCV (Open, High, Low, Close, Volume) data.
2.  **EMA Calculation**: Computes 20, 50, and 200-period Exponential Moving Averages (EMAs).
3.  **Crossover Detection**: Identifies Bullish (20/50), Bearish (20/50), Golden (50/200), and Death (50/200) cross events.
4.  **Historical Analysis**: Calculates and visualizes the time intervals between consecutive crossover events for each type.
5.  **Cloud Integration**: Stores all processed data in Google BigQuery for robust storage and efficient querying.
6.  **Interactive Dashboard**: Presents key insights through a dynamic Power BI dashboard, scheduled for daily updates.

## Table of Contents

  - [Project Overview](https://www.google.com/search?q=%23project-overview)
  - [Features](https://www.google.com/search?q=%23features)
  - [Architecture](https://www.google.com/search?q=%23architecture)
  - [Getting Started](https://www.google.com/search?q=%23getting-started)
      - [Prerequisites](https://www.google.com/search?q=%23prerequisites)
      - [Installation and Setup](https://www.google.com/search?q=%23installation-and-setup)
  - [Usage](https://www.google.com/search?q=%23usage)
  - [Power BI Dashboard Details](https://www.google.com/search?q=%23power-bi-dashboard-details)
  - [Data Model](https://www.google.com/search?q=%23data-model)
  - [Scheduling and Automation](https://www.google.com/search?q=%23scheduling-and-automation)
  - [How This Project Brings Value & Usage for the Trading Team](https://www.google.com/search?q=%23how-this-project-brings-value-%26-usage-for-the-trading-team)
  - [Contribution](https://www.google.com/search?q=%23contribution)
  - [License](https://www.google.com/search?q=%23license)
  - [Contact](https://www.google.com/search?q=%23contact)

## Features

  * **Automated Daily Data Fetching**: Retrieves historical **daily candle** BTC-USD data directly from reliable sources (Yahoo Finance initially, and for more granular and diverse data, `ccxt` for exchange data like Binance).
  * **Dynamic EMA Calculation**: Automatically computes 20, 50, and 200-period EMAs based on daily closing prices.
  * **Comprehensive Crossover Detection**: Identifies four key EMA crossover types:
      * **BULLISH CROSS (20/50)**: 20-EMA crosses above 50-EMA.
      * **BEARISH CROSS (20/50)**: 20-EMA crosses below 50-EMA.
      * **GOLDEN CROSS (50/200)**: 50-EMA crosses above 200-EMA.
      * **DEATH CROSS (50/200)**: 50-EMA crosses below 200-EMA.
  * **Interval Analysis**: Provides detailed insights into the time (in days) between successive occurrences of each crossover type. This is crucial for understanding the historical frequency and duration of market phases.
  * **BigQuery Integration**: Seamlessly loads processed data (crossover events, interval analysis, and raw OHLCV with EMAs from the `ccxt` script) into Google BigQuery tables.
  * **Interactive Power BI Dashboard**: A user-friendly dashboard to visualize:
      * BTC price action with EMA lines (based on daily data).
      * Tables summarizing all historical crossovers (Bullish, Bearish, Golden, Death) including the date, type, and price at the time of the event.
      * Metrics on the time between consecutive crossovers for each type (Average, Median, Min, Max, Standard Deviation, Count).
      * A "Days Since Last Cross" card, providing real-time relevance.
      * A "Current Cross Type" card, indicating the most recent significant crossover.
      * A table showing the distribution of crossovers by year, allowing for historical comparisons.

## Architecture

The project follows a typical data pipeline architecture:

```
+----------------+      +-----------------+      +-----------------+      +-----------------+      +-------------------+
| Data Sources   |----->| Python Scripts  |----->| Google BigQuery |----->| Power BI Desktop  |----->| Power BI Service  |
| (yfinance,     |      | (Daily Data     |      | (Data Warehouse)|      | (Dashboard Design)|      | (Cloud Dashboard, |
| CCXT - Binance)|      | Fetching, EMA   |      |                 |      |                   |      | Daily Refresh)    |
|                |      | Calc, Crossover |      |                 |      |                   |      |                   |
|                |      | Det.)           |      |                 |      |                   |      |                   |
+----------------+      +-----------------+      +-----------------+      +-----------------+      +-------------------+
        ^                                                                                                   |
        |                                                                                                   |
        +---------------------------------------------------------------------------------------------------+
                                            Scheduled Task (Windows Task Scheduler)
                                            for Daily Data Load
```

## Getting Started

### Prerequisites

To run this project, you will need:

  * **Python 3.x**: Ensure Python is installed on your system.
  * **pip**: Python package installer.
  * **Google Cloud Platform (GCP) Project**: With BigQuery API enabled.
  * **GCP Service Account Key**: A JSON key file with permissions to write to BigQuery.
  * **Power BI Desktop**: To open and interact with the Power BI dashboard file.
  * **Power BI Pro/Premium Account (Optional but Recommended)**: For publishing to Power BI Service and setting up scheduled refreshes.

### Installation and Setup

1.  **Clone the Repository**:

    ```bash
    git clone <your-repository-url>
    cd <your-repository-name>
    ```

2.  **Install Python Dependencies**:

    ```bash
    pip install yfinance pandas matplotlib ccxt google-cloud-bigquery pandas-gbq google-auth-oauthlib
    ```

3.  **Google Cloud Service Account Setup**:

      * Go to your Google Cloud Console.
      * Navigate to `IAM & Admin` \> `Service Accounts`.
      * Create a new service account or select an existing one.
      * Ensure the service account has the `BigQuery Data Editor` and `BigQuery Job User` roles for the dataset you intend to use.
      * Create a new JSON key for this service account and download it.
      * **Rename this downloaded JSON file to `ema-analyzer-key.json` and place it in the same directory as your Python scripts.** This is crucial for the scripts to find your credentials.

4.  **BigQuery Dataset**:
    The scripts are configured to use a dataset named `emas_signals`. If it doesn't exist in your BigQuery project, the Python script will attempt to create it (this part is commented out by default but can be uncommented if needed in the Python script itself).

## Usage

1.  **Run the Data Ingestion Scripts**:
    There are two Python scripts:

      * `main.py` (or similar name for the first code snippet): Fetches **daily candle** data from Yahoo Finance, calculates EMAs and crossovers, performs interval analysis, and uploads `crossover_summary`, `crossover_intervals`, `crossover_interval_summary`, and `crossover_interval_distribution` DataFrames to BigQuery.
      * `fetch_raw_ohlcv.py` (or similar name for the second code snippet): Fetches raw **daily candle** OHLCV data with EMAs from a cryptocurrency exchange (Binance by default using `ccxt`) and uploads it as `raw_ohlcv_emas` to BigQuery.

    Execute these scripts from your terminal:

    ```bash
    python main.py
    python fetch_raw_ohlcv.py
    ```

    These scripts will connect to BigQuery using your `ema-analyzer-key.json` and populate the tables.

2.  **Open the Power BI Dashboard**:

      * Open the Power BI `.pbix` file (e.g., `EMACrossoverDashboard.pbix`) in Power BI Desktop.
      * The dashboard will connect to your BigQuery dataset. You might be prompted to sign in with your Google account associated with the GCP project, or it might automatically use the credentials configured through Power BI's BigQuery connector.
      * Refresh the data to ensure you have the latest information from BigQuery.

## Power BI Dashboard Details

The Power BI dashboard consists of two main tabs:

### Tab 1: BTC Price & EMAs

  * **Visual**: A line chart displaying the historical BTC-USD **daily** close price alongside the 20, 50, and 200 EMA lines.
  * **Purpose**: Provides a quick visual overview of **daily** price trends and EMA interactions.

![BTC Price and EMAs Dashboard Tab 1](dashboard1.png)

### Tab 2: Crossover Analysis & Metrics

This tab is designed for in-depth analysis of crossover events.

  * **Top Tables (4 tables)**:

      * **BULLISH CROSS (20/50)**: Lists all detected 20/50 bullish crossovers, showing `Category`, `Previous Date`, `Current Date`, and `Days Between` these specific bullish events.
      * **BEARISH CROSS (20/50)**: Same structure as above, but for 20/50 bearish crossovers.
      * **GOLDEN CROSS (50/200)**: Details for 50/200 golden crosses.
      * **DEATH CROSS (50/200)**: Details for 50/200 death crosses.
      * **Interpretation**: These tables measure the time between *consecutive occurrences of the same type of cross*. For example, the "Days Between" for a Bullish Cross (20/50) indicates how many days passed since the *last* Bullish Cross (20/50) occurred, even if other types of crosses (e.g., bearish) happened in between. This helps understand the cyclical nature and frequency of specific signal types based on **daily candles**.

  * **Summary Statistics (below tables)**:

      * A combined table showing aggregated statistics for each crossover `Category`: `Avg Days Between`, `Median Days Between`, `Min Days Between`, `Max Days Between`, `Interval Count`, and `Std Dev Days`.
      * **Purpose**: Provides quantitative insights into the typical duration between these market signals, helping to gauge their historical periodicity and volatility when analyzed on a **daily basis**.

  * **Key Metrics Cards**:

      * **Days Since Last Cross**: A card displaying the number of days since the most recent crossover of *any* type occurred.
      * **Current Cross Type**: A card indicating the type of the very last crossover event, giving an immediate understanding of the market's most recent major signal. This value dynamically updates.

  * **Crossovers by Year Table**:

      * A table allowing users to filter and view the count and details of crossovers that occurred in specific years.
      * **Purpose**: Enables historical comparison of crossover frequency and types year-over-year, based on **daily data**.

*(Include a screenshot of this tab here if you're hosting on GitHub)*

## Data Model

The Power BI dashboard connects to the following tables in your `emas_signals` BigQuery dataset:

  * **`crossover_summary`**: Contains a list of all detected crossover events (derived from **daily candles**), their date, type, and price.
  * **`crossover_intervals`**: Detailed breakdown of the time elapsed between consecutive events for each specific crossover category.
  * **`crossover_interval_summary`**: Aggregated statistics (mean, median, min, max, std dev) for the `Days Between` intervals for each crossover type.
  * **`crossover_interval_distribution`**: Provides a frequency distribution of the `Days Between` intervals across defined bins for each crossover type.
  * **`raw_ohlcv_emas`**: (Optional, if using the `ccxt` script) Contains the raw **daily** OHLCV data along with the calculated EMA lines. This table serves as the primary data source for the first tab of the dashboard.

The relationships in Power BI are straightforward, primarily relating the tables on `Category` and `Date` where applicable, allowing for filtering and cross-analysis.

## Scheduling and Automation

To ensure the dashboard is always up-to-date, the following automation is implemented:

  * **Python Script Automation**: The Python scripts (`main.py` and `fetch_raw_ohlcv.py`) are scheduled to run daily using **Windows Task Scheduler**. This ensures that the latest BTC-USD **daily candle** data, EMA calculations, and crossover events are computed and uploaded to Google BigQuery every day.
  * **Power BI Scheduled Refresh**: The Power BI dashboard is published to Power BI Service. A **scheduled refresh** is configured in Power BI Service to automatically pull the updated data from BigQuery daily. This ensures that the Power BI dashboard displays the most current market insights without manual intervention.

This setup creates a robust and low-maintenance data pipeline, from raw data collection to interactive visualization, keeping the analysis fresh and relevant.

## How This Project Brings Value & Usage for the Trading Team

This EMA Crossover Analysis Dashboard is a powerful tool designed to enhance your trading strategies and provide a deeper understanding of BTC-USD's price dynamics. Here's how your trading team can leverage it:

1.  **Identifying Potential Trend Reversals (20/50 Crosses):**

      * **BULLISH CROSS (20/50) (Tab 2 Tables & Current Cross Card):** When the 20-EMA crosses above the 50-EMA, it often signals a shift from a short-term downtrend to an uptrend. Traders can use this as an early indication to look for long opportunities or to tighten stops on short positions.
      * **BEARISH CROSS (20/50) (Tab 2 Tables & Current Cross Card):** Conversely, a 20-EMA crossing below the 50-EMA suggests a potential shift from an uptrend to a downtrend. This can be used to consider shorting opportunities or to take profits on long positions.
      * **Actionable Insight:** Monitor the "Current Cross Type" card daily for immediate signals. Use the "Days Since Last Cross" to gauge the recency of the last major short-term momentum shift.

2.  **Gauging Long-Term Market Health (50/200 Crosses):**

      * **GOLDEN CROSS (50/200) (Tab 2 Tables):** This is a strong bullish signal, often indicating the start of a significant long-term uptrend. It's a key indicator for long-term investors and swing traders.
      * **DEATH CROSS (50/200) (Tab 2 Tables):** This is a strong bearish signal, often indicating the start of a significant long-term downtrend. It's crucial for risk management and identifying potential bear markets.
      * **Actionable Insight:** These crosses provide a macro view. Use them to confirm broader market sentiment and adjust your overall trading bias (e.g., more aggressive long positions during Golden Cross periods, more cautious or short-biased during Death Cross periods).

3.  **Understanding Historical Cycle Durations (Interval Analysis Tables):**

      * **"Days Between" Tables (Tab 2):** By observing the `Days Between` column for each crossover type, you can understand how often these events have historically occurred. For example, if Bullish 20/50 crosses historically happen every 30-60 days, and it's been 80 days since the last one, it might indicate a prolonged trend or that a new signal is due.
      * **Summary Statistics (Tab 2):** The `Avg Days Between`, `Median Days Between`, `Min Days Between`, `Max Days Between`, and `Std Dev Days` provide statistical insights into the regularity and variability of these signals.
      * **Actionable Insight:** Use these statistics to set expectations for trend durations. For instance, if the average time between Golden Crosses is X days, and a current Golden Cross has been active for X days, you might start looking for signs of weakening momentum or a potential Death Cross. This provides a data-driven historical context to current market movements.

4.  **Contextualizing Current Signals with Historical Patterns (Crossovers by Year Table):**

      * **Crossovers by Year (Tab 2):** This table allows you to compare the frequency and types of crossovers in the current year versus previous years. Are there more bullish crosses this year than last? Fewer bearish crosses?
      * **Actionable Insight:** This helps put current market behavior into historical context. If this year shows an unusually low number of signals, it might suggest a period of consolidation. If there's an increase in volatility (more frequent crosses), it might warrant a more agile trading approach.

5.  **Risk Management & Position Sizing:**

      * While not explicit trading signals on their own, EMAs and crossovers provide context. A bearish cross might prompt a review of existing long positions, potentially leading to taking profits or setting tighter stop-losses.
      * **Actionable Insight:** Integrate the dashboard's insights into your risk management framework. For example, if a Death Cross occurs, it might signal a period where you reduce your overall market exposure or take smaller position sizes on long trades.

**In summary, this dashboard empowers the trading team by providing:**

  * **Clear, Automated Signals**: No manual calculation needed; signals are refreshed daily.
  * **Historical Context**: Understand how current market events compare to past cycles.
  * **Data-Driven Decision Making**: Move beyond gut feelings by incorporating quantitative insights into your trading plan.
  * **Early Warning System**: Identify potential shifts in momentum before they become obvious.

By regularly checking this dashboard, especially Tab 2 for interval analysis and the key metrics, your team can refine entries and exits, manage risk more effectively, and gain a competitive edge in the BTC-USD market.

-----
