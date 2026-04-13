import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
)

cursor = conn.cursor()
cursor.execute("SELECT CURRENT_VERSION()")
row = cursor.fetchone()
print(f"Connected successfully! Snowflake version: {row[0]}")

cursor.close()
conn.close()