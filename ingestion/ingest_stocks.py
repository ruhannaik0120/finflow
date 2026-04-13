import requests
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

# Config
API_KEY = os.getenv('ALPHA_VANTAGE_KEY')

TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'JPM', 'TSLA']

#Connect to Snowflake
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
)
cursor = conn.cursor()

# Make sure we are writing to the right schema
cursor.execute("USE SCHEMA FINFLOW_DB.RAW")

#Fetch and load each ticker
def fetch_and_load(ticker):
    print(f"Fetching data for {ticker}")

    url = (
        f"https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY"
        f"&symbol={ticker}"
        f"&outputsize=compact"
        f"&apikey={API_KEY}"
    )

    response = requests.get(url)
    data = response.json()

    # Alpha Vantage returns data inside this key
    if "Time Series (Daily)" not in data:
        print(f"  Could not fetch {ticker}. Response: {data}")
        return

    time_series = data["Time Series (Daily)"]

    # Convert to a pandas dataframe
    rows = []
    for date_str, values in time_series.items():
        rows.append({
            "ticker":      ticker,
            "trade_date":  date_str,
            "open_price":  float(values["1. open"]),
            "high_price":  float(values["2. high"]),
            "low_price":   float(values["3. low"]),
            "close_price": float(values["4. close"]),
            "volume":      int(values["5. volume"])
        })

    df = pd.DataFrame(rows)
    print(f"  Got {len(df)} rows. Loading into Snowflake")

    # Load each row into Snowflake
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO raw_stock_prices
                (ticker, trade_date, open_price, high_price,
                 low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row['ticker'],
            row['trade_date'],
            row['open_price'],
            row['high_price'],
            row['low_price'],
            row['close_price'],
            row['volume']
        ))

    print(f"  Done loading {ticker}.")

#Run for all tickers 
for ticker in TICKERS:
    fetch_and_load(ticker)

cursor.close()
conn.close()
print("\nData loaded into Snowflake.")