import requests
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

#Config 
FRED_KEY = os.getenv('FRED_KEY')

# macro indicators FRED series ID
INDICATORS = {
    'FEDFUNDS':  'Federal Funds Rate',       # US interest rate
    'CPIAUCSL':  'Consumer Price Index',     # Inflation
    'GDP':       'Gross Domestic Product',   # US GDP
    'UNRATE':    'Unemployment Rate',        # US unemployment
    'DGS10':     '10 Year Treasury Rate'     # 10 year bond yield
}

# Connect to Snowflake 
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
)
cursor = conn.cursor()
cursor.execute("USE SCHEMA FINFLOW_DB.RAW")

#Fetch and load each indicator
def fetch_and_load(series_id, indicator_name):
    print(f"Fetching {indicator_name}...")

    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}"
        f"&api_key={FRED_KEY}"
        f"&file_type=json"
        f"&limit=100"
        f"&sort_order=desc"
    )

    response = requests.get(url)
    data = response.json()

    if "observations" not in data:
        print(f"  Could not fetch {indicator_name}. Response: {data}")
        return

    rows = []
    for obs in data["observations"]:
        if obs["value"] == ".":
            continue
        rows.append({
            "indicator_name": indicator_name,
            "indicator_date": obs["date"],
            "value":          float(obs["value"])
        })

    df = pd.DataFrame(rows)
    print(f"  Got {len(df)} rows. Loading into Snowflake...")

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO raw_macro_indicators
                (indicator_name, indicator_date, value)
            VALUES (%s, %s, %s)
        """, (
            row['indicator_name'],
            row['indicator_date'],
            row['value']
        ))

    print(f"  Done loading {indicator_name}.")

# Run for all indicators
for series_id, name in INDICATORS.items():
    fetch_and_load(series_id, name)

cursor.close()
conn.close()
print("\nAll done! Macro data loaded into Snowflake.")