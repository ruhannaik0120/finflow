# FinFlow — Financial Data Engineering Pipeline

A end-to-end financial data engineering project built with Python, Snowflake, dbt, and Streamlit.

## What This Project Does

FinFlow is a fully automated financial analytics pipeline that:
- Ingests daily stock prices for multiple tickers from Alpha Vantage
- Ingests macroeconomic indicators from FRED (Federal Reserve Economic Data)
- Transforms raw data into analytical models using dbt
- Detects anomalies and sends automated email alerts via Snowflake
- Visualizes everything in an interactive Streamlit dashboard

## Architecture
Alpha Vantage API → Python Ingestion → Snowflake RAW
FRED API         → Python Ingestion → Snowflake RAW
↓
dbt Models
↓
Snowflake MART
↓
Streamlit Dashboard

## Tech Stack

- **Python** — data ingestion scripts
- **Snowflake** — cloud data warehouse with three schemas (RAW, STAGING, MART)
- **dbt** — data transformation and testing
- **Streamlit** — interactive dashboard
- **Plotly** — interactive charts
- **Alpha Vantage** — stock price data
- **FRED** — macroeconomic data
- **Windows Task Scheduler** — automated daily pipeline runs

## Project Structure
finflow/
├── ingestion/
│   ├── ingest_stocks.py      # Fetches stock prices from Alpha Vantage
│   └── ingest_macro.py       # Fetches macro indicators from FRED
├── finflow_dbt/
│   ├── models/
│   │   ├── staging/          # stg_stock_prices, stg_macro_indicators
│   │   └── mart/             # mart_portfolio, mart_risk, mart_macro_correlation
│   └── dbt_project.yml
├── dashboard/
│   └── app.py                # Streamlit dashboard
├── run_pipeline.bat           # Automated pipeline script
└── .env                       # API keys and credentials (not committed)

## dbt Models

| Model | Type | Description |
|---|---|---|
| stg_stock_prices | View | Cleaned stock price data from RAW |
| stg_macro_indicators | View | Cleaned macro indicator data from RAW |
| mart_portfolio | Table | Price history with moving averages, volatility, drawdown |
| mart_risk | Table | Risk metrics including Sharpe ratio and VaR |
| mart_macro_correlation | Table | Rolling correlation between stocks and macro indicators |

## Snowflake Alerts

Three automated alerts run on Snowflake:

- **alert_price_spike** — fires when a stock moves more than 2 standard deviations from its 20-day moving average
- **alert_pipeline_failure** — fires every weekday morning if no data was ingested for today
- **alert_drawdown_breach** — fires when any position drops more than 10% from its peak

## Dashboard Features

- Stock price history chart for all tracked tickers
- Moving average chart (20-day and 50-day) per ticker
- Risk metrics — rolling volatility and Sharpe ratio
- Macro correlation analysis
- Portfolio summary metrics — best performer, worst performer, most volatile
- Dynamic ticker addition — type any ticker and the dashboard fetches, ingests and displays it automatically
- One-click portfolio summary email report

## Setup

1. Clone the repository
2. Install dependencies
```bash
pip install snowflake-connector-python pandas requests python-dotenv dbt-snowflake streamlit plotly
```
3. Create a `.env` file with your credentials
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=FINFLOW_DB
SNOWFLAKE_WAREHOUSE=FINFLOW_WH
ALPHA_VANTAGE_KEY=your_key
FRED_API_KEY=your_key
4. Run the ingestion scripts
```bash
python ingestion/ingest_stocks.py
python ingestion/ingest_macro.py
```
5. Run dbt
```bash
cd finflow_dbt
dbt run
dbt test
```
6. Launch the dashboard
```bash
cd dashboard
python -m streamlit run app.py
```

## Data Quality

15 dbt tests run on every pipeline execution covering:
- Not null checks on all key columns
- Accepted values checks on indicator names
- Referential integrity across models