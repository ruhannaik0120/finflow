import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import os

load_dotenv()

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="FINFLOW_WH",
        database="FINFLOW_DB",
        schema="MART"
    )

@st.cache_data(ttl=3600)
def load_data(query):
    conn = get_connection()
    return pd.read_sql(query, conn)

st.set_page_config(
    page_title="FinFlow Dashboard",
    page_icon="📈",
    layout="wide"
)

st.title("FinFlow Financial Dashboard")
st.markdown("Real-time financial analytics powered by Snowflake, dbt, and Python")
st.divider()

st.sidebar.title("Filters")
st.sidebar.markdown("Use the filters below to explore the data")

ticker_options = load_data("""
    SELECT DISTINCT ticker 
    FROM FINFLOW_DB.MART.mart_portfolio 
    ORDER BY ticker
""")["TICKER"].tolist()

selected_tickers = st.sidebar.multiselect(
    "Select Tickers",
    options=ticker_options,
    default=ticker_options
)

st.sidebar.divider()
st.sidebar.subheader("Add New Ticker")

new_ticker = st.sidebar.text_input("Enter Ticker Symbol", placeholder="eg NFLX").upper()

if st.sidebar.button("Add Ticker"):
    if new_ticker == "":
        st.sidebar.error("Please enter a ticker symbol")
    else:
        with st.spinner(f"Fetching data for {new_ticker}..."):
            import subprocess
            ingest_path = r"C:\Users\ruhan\OneDrive\Desktop\finflow\ingestion\ingest_stocks.py"
            dbt_path = r"C:\Users\ruhan\OneDrive\Desktop\finflow\finflow_dbt"
            
            ingest_result = subprocess.run(
                ["python", ingest_path, new_ticker],
                capture_output=True, text=True
            )
            
            if ingest_result.returncode != 0:
                st.sidebar.error(f"Ingestion failed: {ingest_result.stderr}")
            else:
                dbt_result = subprocess.run(
                    ["dbt", "run"],
                    capture_output=True, text=True,
                    cwd=dbt_path
                )
                if dbt_result.returncode != 0:
                    st.sidebar.error(f"dbt failed: {dbt_result.stderr}")
                else:
                    st.sidebar.success(f"{new_ticker} added successfully!")
                    st.cache_data.clear()
                    st.rerun()

portfolio_df = load_data("""
    SELECT 
        ticker,
        trade_date,
        close_price,
        ma_20day,
        ma_50day,
        rolling_volatility_20d,
        daily_return_pct
    FROM FINFLOW_DB.MART.mart_portfolio
    ORDER BY trade_date
""")

portfolio_df = portfolio_df[portfolio_df["TICKER"].isin(selected_tickers)]

st.subheader("Portfolio Summary")

summary = load_data("""
    SELECT
        ticker,
        MAX(close_price) as max_price,
        MIN(close_price) as min_price,
        ROUND(AVG(rolling_volatility_20d), 2) as avg_volatility,
        ROUND(MAX(cumulative_return_pct), 2) as best_return,
        ROUND(MIN(drawdown_pct), 2) as max_drawdown
    FROM FINFLOW_DB.MART.mart_portfolio
    WHERE ticker IN ({})
    GROUP BY ticker
""".format(",".join(f"'{t}'" for t in ticker_options)))

best_ticker = summary.loc[summary["BEST_RETURN"].idxmax(), "TICKER"]
worst_ticker = summary.loc[summary["BEST_RETURN"].idxmin(), "TICKER"]
most_volatile = summary.loc[summary["AVG_VOLATILITY"].idxmax(), "TICKER"]
best_return_val = summary["BEST_RETURN"].max()
worst_return_val = summary["BEST_RETURN"].min()
highest_vol = summary["AVG_VOLATILITY"].max()

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(label="Best Performer", value=best_ticker, delta=f"{best_return_val}%")
with col2:
    st.metric(label="Worst Performer", value=worst_ticker, delta=f"{worst_return_val}%")
with col3:
    st.metric(label="Most Volatile", value=most_volatile)
with col4:
    st.metric(label="Avg Volatility", value=f"{highest_vol}%")
with col5:
    st.metric(label="Tickers Tracked", value=len(ticker_options))

st.subheader("Stock Price History")

fig_price = px.line(
    portfolio_df,
    x="TRADE_DATE",
    y="CLOSE_PRICE",
    color="TICKER",
    title="Close Price Over Time"
)

st.plotly_chart(fig_price, use_container_width=True)

st.subheader("20-Day vs 50-Day Moving Average")

selected_ticker_ma = st.selectbox("Select Ticker for Moving Average", options=selected_tickers)

ma_df = portfolio_df[portfolio_df["TICKER"] == selected_ticker_ma]

fig_ma = go.Figure()

fig_ma.add_trace(go.Scatter(x=ma_df["TRADE_DATE"], y=ma_df["CLOSE_PRICE"], name="Close Price", line=dict(color="white")))
fig_ma.add_trace(go.Scatter(x=ma_df["TRADE_DATE"], y=ma_df["MA_20DAY"], name="20-Day MA", line=dict(color="orange")))
fig_ma.add_trace(go.Scatter(x=ma_df["TRADE_DATE"], y=ma_df["MA_50DAY"], name="50-Day MA", line=dict(color="cyan")))

fig_ma.update_layout(title=f"{selected_ticker_ma} Price vs Moving Averages")

st.plotly_chart(fig_ma, use_container_width=True)

st.divider()
st.subheader("Risk Metrics")

risk_df = load_data("""
    SELECT
        ticker,
        trade_date,
        rolling_volatility_20d,
        sharpe_ratio_20d,
        max_drawdown_pct,
        var_95_pct,
        cumulative_return_pct
    FROM FINFLOW_DB.MART.mart_risk
    ORDER BY trade_date
""")

risk_df = risk_df[risk_df["TICKER"].isin(selected_tickers)]

col1, col2 = st.columns(2)

with col1:
    fig_vol = px.line(
        risk_df,
        x="TRADE_DATE",
        y="ROLLING_VOLATILITY_20D",
        color="TICKER",
        title="20-Day Rolling Volatility"
    )
    st.plotly_chart(fig_vol, use_container_width=True)

with col2:
    fig_sharpe = px.line(
        risk_df,
        x="TRADE_DATE",
        y="SHARPE_RATIO_20D",
        color="TICKER",
        title="20-Day Sharpe Ratio"
    )
    st.plotly_chart(fig_sharpe, use_container_width=True)

st.divider()
st.subheader("Macro Correlation")

macro_df = load_data("""
    SELECT
        ticker,
        trade_date,
        indicator_name,
        indicator_value,
        rolling_correlation_20d
    FROM FINFLOW_DB.MART.mart_macro_correlation
    ORDER BY trade_date
""")

macro_df = macro_df[macro_df["TICKER"].isin(selected_tickers)]

macro_df["TRADE_DATE"] = pd.to_datetime(macro_df["TRADE_DATE"]).dt.date

selected_indicator = st.selectbox(
    "Select Macro Indicator",
    options=macro_df["INDICATOR_NAME"].unique()
)

macro_filtered = macro_df[macro_df["INDICATOR_NAME"] == selected_indicator]

has_correlation = macro_filtered["ROLLING_CORRELATION_20D"].notna().any()

if has_correlation:
    fig_macro = px.line(
        macro_filtered,
        x="TRADE_DATE",
        y="ROLLING_CORRELATION_20D",
        color="TICKER",
        title=f"20-Day Rolling Correlation with {selected_indicator}"
    )
    st.plotly_chart(fig_macro, use_container_width=True)

else:
    st.markdown(f"**Latest {selected_indicator} Values**")
    cols = st.columns(len(macro_filtered))
    for i, (_, row) in enumerate(macro_filtered.iterrows()):
        with cols[i]:
            st.metric(
                label=row["TICKER"],
                value=round(row["INDICATOR_VALUE"], 2)
            )

