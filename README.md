# FinFlow — Financial Data Engineering Pipeline

FinFlow is a production-oriented financial data pipeline built with Python, Snowflake, dbt, Streamlit, and Plotly. It ingests daily Alpha Vantage stock prices and FRED macroeconomic observations, transforms them through RAW, STAGING, and MART schemas, and presents portfolio, risk, macro-correlation, and pipeline-health views.

## Architecture

```text
Alpha Vantage ─┐
               ├─ Python normalization + transactional MERGE ─ Snowflake RAW
FRED ──────────┘                                               │
                                                               ├─ pipeline_logs
                                                               └─ dbt build
                                                                    ├─ STAGING
                                                                    └─ MART ─ Streamlit
```

The five existing dbt models and their financial calculations are preserved:

| Model | Type | Purpose |
|---|---|---|
| `stg_stock_prices` | View | Typed, deduplicated stock prices |
| `stg_macro_indicators` | View | Typed, deduplicated macro observations |
| `mart_portfolio` | Table | Returns, moving averages, volatility, and drawdown |
| `mart_risk` | Table | Sharpe ratio, VaR, and maximum drawdown |
| `mart_macro_correlation` | Table | Stock/macro rolling correlations |

Snowflake alerts remain compatible with the RAW/STAGING/MART design.

## Reliable ingestion

Each stock ticker and each macro indicator is an isolated entity run with its own UUID and UTC timestamps. Importing an ingestion module does not connect, fetch, execute SQL, or start a run.

Normalization is deterministic and never mutates the caller's DataFrame. Stock rows require a conservative ticker, valid date, positive and internally consistent OHLC values, and a positive integral volume. Prices are rounded to four decimal places. Macro rows require bounded series/name values, a valid date, and a finite numeric value rounded to six decimal places. Duplicate source keys keep the final occurrence in original input order.

Normalized rows are loaded with bound parameters into a UUID-named session temporary table. A conditional Snowflake `MERGE` then:

- matches stocks by `ticker, trade_date` and updates only changed OHLCV values;
- matches macro data by `indicator_name, indicator_date` and updates only changed values;
- inserts explicit target columns for new keys;
- updates `loaded_at` only when business values change.

Snowflake's returned MERGE column names are interpreted case-insensitively without relying on tuple position. Counts must be non-negative integral values. An identical rerun inserts and updates zero rows and reports every valid row as unchanged.

The pipeline creates `FINFLOW_DB.RAW.pipeline_logs` with `CREATE TABLE IF NOT EXISTS`. It records pipeline/entity identity, fetched/valid/inserted/updated/unchanged/dropped counts, duration, status, safe error code/message, and start/completion timestamps. The temporary-row load, target MERGE, and successful audit insert share one explicit transaction and one commit. A load or audit failure rolls back that transaction; a failed outcome is then attempted through a clean connection.

Statuses are deterministic:

- `SUCCESS`: MERGE changed data and no rows were dropped.
- `PARTIAL_SUCCESS`: MERGE changed data and normalization dropped rows.
- `NO_CHANGES`: valid rows were identical to target data.
- `NO_VALID_ROWS`: normalization rejected every fetched row; no MERGE ran.
- `FAILED`: configuration, API, normalization, Snowflake load, or audit processing failed.

One entity failure does not prevent later default tickers or macro indicators from running. The aggregate script exits nonzero if any entity failed.

## Configuration

Create a local `.env` file (it is ignored by Git):

```dotenv
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=FINFLOW_DB
SNOWFLAKE_WAREHOUSE=FINFLOW_WH
ALPHA_VANTAGE_KEY=your_key
FRED_API_KEY=your_key
```

`FRED_API_KEY` is canonical. `FRED_KEY` remains a temporary fallback and emits a value-free deprecation warning. Missing configuration reports variable names only. HTTP requests use bound query parameters, a 30-second timeout, status validation, safe JSON parsing, fixed error codes, and no raw API response or key-bearing URL logging.

Install only the existing project dependencies:

```bash
python -m pip install snowflake-connector-python pandas requests python-dotenv dbt-snowflake streamlit plotly
```

## Running the pipeline

From the repository root:

```bash
python run_pipeline.py
```

This runs the five default stock tickers, all five macro indicators, then `dbt build`. dbt is not started if ingestion fails.

To add or refresh one ticker without rerunning macro ingestion:

```bash
python run_pipeline.py --ticker NFLX
```

`run_pipeline.bat` delegates to the same repository-relative runner and propagates its exit code. The runner uses the active Python interpreter for ingestion, resolves dbt beside that interpreter when available, avoids a shell, applies bounded timeouts, and reports only safe stage-level results.

The individual ingestion commands remain available:

```bash
python ingestion/ingest_stocks.py
python ingestion/ingest_stocks.py NFLX
python ingestion/ingest_macro.py
```

## Dashboard

Launch Streamlit with:

```bash
python -m streamlit run dashboard/app.py
```

The existing portfolio summary, stock history, moving averages, risk charts, macro analysis, dynamic ticker addition, and portfolio email action remain. Dynamic ticker ingestion validates the symbol and calls the portable runner using the active Python interpreter, a repository-relative path, no shell, and a bounded timeout. Analytics cache is cleared only after a successful pipeline run. Ticker filtering uses bound SQL parameters and the selected ticker set.

The **Pipeline Operations** expander shows the latest overall state and completion time, latest successful stock and macro runs, recent inserted/updated/unchanged/dropped totals, failures over seven days, and the latest 20 entity runs. Error messages are truncated and missing audit infrastructure is handled with a fixed safe message.

## Data quality and freshness

`dbt build` is the transformation and quality gate. The project has **28 dbt tests**: 23 schema tests plus five singular SQL tests for stock-key uniqueness, valid OHLCV values, macro-key uniqueness, audit-count invariants, and supported audit statuses/pipeline names.

Stock-source freshness warns after three days and errors after seven days so weekends and market holidays are tolerated. Monthly and quarterly macro series intentionally have no unrealistic source freshness threshold; macro pipeline execution health is visible through `pipeline_logs`.

The fake-only standard-library suite has **65 Python tests** and does not call live APIs or Snowflake:

```bash
python -m unittest discover -s tests -v
python -m compileall ingestion dashboard run_pipeline.py
cd finflow_dbt
dbt parse --no-partial-parse
```

Do not run ingestion or `dbt build` against a live target as part of ordinary static verification.
