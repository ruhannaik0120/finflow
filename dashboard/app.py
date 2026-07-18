"""FinFlow Streamlit analytics and pipeline operations dashboard."""

import os
from pathlib import Path
import re
import subprocess
import sys

from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st


load_dotenv()


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="MART",
    )


@st.cache_data(ttl=3600)
def load_data(query, params=()):
    return pd.read_sql_query(query, get_connection(), params=params)


st.set_page_config(page_title="FinFlow Dashboard", page_icon="📈", layout="wide")
st.title("FinFlow Financial Dashboard")
st.markdown("Financial analytics powered by Snowflake, dbt, and Python")
st.divider()

st.sidebar.title("Filters")
st.sidebar.markdown("Use the filters below to explore the data")
ticker_options = load_data("""
    SELECT DISTINCT ticker
    FROM FINFLOW_DB.MART.mart_portfolio
    ORDER BY ticker
""")["TICKER"].tolist()
selected_tickers = st.sidebar.multiselect(
    "Select Tickers", options=ticker_options, default=ticker_options
)

st.sidebar.divider()
st.sidebar.subheader("Add New Ticker")
new_ticker = st.sidebar.text_input("Enter Ticker Symbol", placeholder="eg NFLX").strip().upper()
if st.sidebar.button("Add Ticker"):
    if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,9}", new_ticker):
        st.sidebar.error("Enter a valid ticker symbol (up to 10 letters, numbers, dots, or hyphens).")
    else:
        with st.spinner(f"Fetching data for {new_ticker}..."):
            repository_root = Path(__file__).resolve().parents[1]
            try:
                pipeline_result = subprocess.run(
                    [sys.executable, str(repository_root / "run_pipeline.py"), "--ticker", new_ticker],
                    cwd=str(repository_root),
                    capture_output=True,
                    text=True,
                    timeout=900,
                    shell=False,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                st.sidebar.error("Ticker ingestion timed out. Check Pipeline Operations for details.")
            except OSError:
                st.sidebar.error("Ticker ingestion could not be started.")
            else:
                if pipeline_result.returncode != 0:
                    st.sidebar.error("Ticker ingestion failed. Check Pipeline Operations for details.")
                else:
                    st.sidebar.success(f"{new_ticker} added successfully!")
                    st.cache_data.clear()
                    st.rerun()

st.sidebar.divider()
st.sidebar.subheader("Portfolio Report")
if st.sidebar.button("Send Portfolio Summary Email"):
    with st.spinner("Sending email..."):
        connection = get_connection()
        email_cursor = connection.cursor()
        try:
            email_cursor.execute("CALL send_portfolio_summary()")
            email_cursor.fetchone()
        finally:
            email_cursor.close()
    st.sidebar.success("Email sent to your inbox!")

with st.expander("Pipeline Operations", expanded=False):
    try:
        recent_runs = load_data("""
            SELECT
                run_id, completed_at, pipeline_name, entity_name, status,
                rows_fetched, rows_valid, rows_inserted, rows_updated,
                rows_unchanged, rows_dropped, duration_ms, error_code,
                SUBSTR(error_message, 1, 160) AS error_message
            FROM FINFLOW_DB.RAW.pipeline_logs
            ORDER BY completed_at DESC
            LIMIT 20
        """)
        latest_stock = load_data("""
            SELECT entity_name, completed_at, status
            FROM FINFLOW_DB.RAW.pipeline_logs
            WHERE pipeline_name = 'STOCK_INGESTION'
              AND status IN ('SUCCESS', 'PARTIAL_SUCCESS', 'NO_CHANGES', 'NO_VALID_ROWS')
            ORDER BY completed_at DESC
            LIMIT 1
        """)
        latest_macro = load_data("""
            SELECT entity_name, completed_at, status
            FROM FINFLOW_DB.RAW.pipeline_logs
            WHERE pipeline_name = 'MACRO_INGESTION'
              AND status IN ('SUCCESS', 'PARTIAL_SUCCESS', 'NO_CHANGES', 'NO_VALID_ROWS')
            ORDER BY completed_at DESC
            LIMIT 1
        """)
        failed_recent = load_data("""
            SELECT COUNT(*) AS failed_count
            FROM FINFLOW_DB.RAW.pipeline_logs
            WHERE status = 'FAILED'
              AND completed_at >= DATEADD(day, -7, CURRENT_TIMESTAMP())
        """)
        if recent_runs.empty:
            st.info("No pipeline audit runs are available yet.")
        else:
            latest = recent_runs.iloc[0]
            top1, top2, top3, top4 = st.columns(4)
            top1.metric("Latest Status", latest["STATUS"])
            top2.metric("Latest Completed", str(latest["COMPLETED_AT"]))
            top3.metric(
                "Latest Stock Run",
                "Not available" if latest_stock.empty else latest_stock.iloc[0]["ENTITY_NAME"],
            )
            top4.metric(
                "Latest Macro Run",
                "Not available" if latest_macro.empty else latest_macro.iloc[0]["ENTITY_NAME"],
            )
            metric1, metric2, metric3, metric4, metric5 = st.columns(5)
            metric1.metric("Rows Inserted", int(recent_runs["ROWS_INSERTED"].sum()))
            metric2.metric("Rows Updated", int(recent_runs["ROWS_UPDATED"].sum()))
            metric3.metric("Rows Unchanged", int(recent_runs["ROWS_UNCHANGED"].sum()))
            metric4.metric("Rows Dropped", int(recent_runs["ROWS_DROPPED"].sum()))
            metric5.metric(
                "Failures (7 days)",
                0 if failed_recent.empty else int(failed_recent.iloc[0]["FAILED_COUNT"]),
            )
            st.caption("Most recent 20 entity runs")
            st.dataframe(recent_runs, use_container_width=True, hide_index=True)
    except Exception:
        st.info("Pipeline audit information is not available yet.")

portfolio_df = load_data("""
    SELECT ticker, trade_date, close_price, ma_20day, ma_50day,
           rolling_volatility_20d, daily_return_pct
    FROM FINFLOW_DB.MART.mart_portfolio
    ORDER BY trade_date
""")
portfolio_df = portfolio_df[portfolio_df["TICKER"].isin(selected_tickers)]

st.subheader("Portfolio Summary")
if selected_tickers:
    placeholders = ",".join(["%s"] * len(selected_tickers))
    summary = load_data(
        f"""
        SELECT ticker, MAX(close_price) AS max_price, MIN(close_price) AS min_price,
               ROUND(AVG(rolling_volatility_20d), 2) AS avg_volatility,
               ROUND(MAX(cumulative_return_pct), 2) AS best_return,
               ROUND(MIN(drawdown_pct), 2) AS max_drawdown
        FROM FINFLOW_DB.MART.mart_portfolio
        WHERE ticker IN ({placeholders})
        GROUP BY ticker
        """,
        tuple(selected_tickers),
    )
else:
    summary = pd.DataFrame()

if summary.empty:
    st.info("Select at least one ticker to view portfolio metrics.")
else:
    best_ticker = summary.loc[summary["BEST_RETURN"].idxmax(), "TICKER"]
    worst_ticker = summary.loc[summary["BEST_RETURN"].idxmin(), "TICKER"]
    most_volatile = summary.loc[summary["AVG_VOLATILITY"].idxmax(), "TICKER"]
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Best Performer", best_ticker, f"{summary['BEST_RETURN'].max()}%")
    col2.metric("Worst Performer", worst_ticker, f"{summary['BEST_RETURN'].min()}%")
    col3.metric("Most Volatile", most_volatile)
    col4.metric("Avg Volatility", f"{summary['AVG_VOLATILITY'].max()}%")
    col5.metric("Tickers Tracked", len(selected_tickers))

st.subheader("Stock Price History")
st.plotly_chart(
    px.line(portfolio_df, x="TRADE_DATE", y="CLOSE_PRICE", color="TICKER", title="Close Price Over Time"),
    use_container_width=True,
)

st.subheader("20-Day vs 50-Day Moving Average")
selected_ticker_ma = st.selectbox(
    "Select Ticker for Moving Average",
    options=selected_tickers,
    index=0 if selected_tickers else None,
    placeholder="Select a ticker",
)
ma_df = portfolio_df[portfolio_df["TICKER"] == selected_ticker_ma] if selected_ticker_ma else portfolio_df.iloc[0:0]
fig_ma = go.Figure()
fig_ma.add_trace(go.Scatter(x=ma_df["TRADE_DATE"], y=ma_df["CLOSE_PRICE"], name="Close Price", line=dict(color="white")))
fig_ma.add_trace(go.Scatter(x=ma_df["TRADE_DATE"], y=ma_df["MA_20DAY"], name="20-Day MA", line=dict(color="orange")))
fig_ma.add_trace(go.Scatter(x=ma_df["TRADE_DATE"], y=ma_df["MA_50DAY"], name="50-Day MA", line=dict(color="cyan")))
fig_ma.update_layout(title=f"{selected_ticker_ma or 'Selected Ticker'} Price vs Moving Averages")
st.plotly_chart(fig_ma, use_container_width=True)

st.divider()
st.subheader("Risk Metrics")
risk_df = load_data("""
    SELECT ticker, trade_date, rolling_volatility_20d, sharpe_ratio_20d,
           max_drawdown_pct, var_95_pct, cumulative_return_pct
    FROM FINFLOW_DB.MART.mart_risk
    ORDER BY trade_date
""")
risk_df = risk_df[risk_df["TICKER"].isin(selected_tickers)]
col1, col2 = st.columns(2)
with col1:
    st.plotly_chart(px.line(risk_df, x="TRADE_DATE", y="ROLLING_VOLATILITY_20D", color="TICKER", title="20-Day Rolling Volatility"), use_container_width=True)
with col2:
    st.plotly_chart(px.line(risk_df, x="TRADE_DATE", y="SHARPE_RATIO_20D", color="TICKER", title="20-Day Sharpe Ratio"), use_container_width=True)

st.divider()
st.subheader("Macro Correlation")
macro_df = load_data("""
    SELECT ticker, trade_date, indicator_name, indicator_value, rolling_correlation_20d
    FROM FINFLOW_DB.MART.mart_macro_correlation
    ORDER BY trade_date
""")
macro_df = macro_df[macro_df["TICKER"].isin(selected_tickers)]
macro_df["TRADE_DATE"] = pd.to_datetime(macro_df["TRADE_DATE"]).dt.date
indicator_options = macro_df["INDICATOR_NAME"].dropna().unique()
selected_indicator = st.selectbox(
    "Select Macro Indicator",
    options=indicator_options,
    index=0 if len(indicator_options) else None,
    placeholder="No macro indicators available",
)
macro_filtered = macro_df[macro_df["INDICATOR_NAME"] == selected_indicator] if selected_indicator else macro_df.iloc[0:0]
if macro_filtered["ROLLING_CORRELATION_20D"].notna().any():
    st.plotly_chart(
        px.line(
            macro_filtered,
            x="TRADE_DATE",
            y="ROLLING_CORRELATION_20D",
            color="TICKER",
            title=f"20-Day Rolling Correlation with {selected_indicator}",
        ),
        use_container_width=True,
    )
elif selected_indicator:
    st.markdown(f"**Latest {selected_indicator} Values**")
    latest_values = macro_filtered.sort_values("TRADE_DATE").groupby("TICKER", as_index=False).tail(1)
    columns = st.columns(max(1, len(latest_values)))
    for index, (_, row) in enumerate(latest_values.iterrows()):
        columns[index].metric(row["TICKER"], round(row["INDICATOR_VALUE"], 2))
