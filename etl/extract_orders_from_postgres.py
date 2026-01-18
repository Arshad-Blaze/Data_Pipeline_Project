import pandas as pd
import psycopg2
from datetime import datetime
import os

RAW_DIR = "data/raw"
METADATA_DIR = "metadata"
WATERMARK_FILE = f"{METADATA_DIR}/last_watermark.txt"

DB_CONFIG = {
    "host": "localhost",
    "port": 8080,
    "dbname": "orders_db",
    "user": "postgres",
    "password": "postgres"
}

BASE_QUERY = """
SELECT
    order_id,
    customer_id,
    product_id,
    order_status,
    order_amount,
    updated_at
FROM orders
"""

INCREMENTAL_QUERY = """
SELECT
    order_id,
    customer_id,
    product_id,
    order_status,
    order_amount,
    updated_at
FROM orders
WHERE updated_at > %s
"""

def read_last_watermark():
    if not os.path.exists(WATERMARK_FILE):
        return None
    with open(WATERMARK_FILE, "r") as f:
        return f.read().strip()

def write_new_watermark(ts):
    os.makedirs(METADATA_DIR, exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        f.write(ts)

def extract():
    conn = psycopg2.connect(**DB_CONFIG)

    last_watermark = read_last_watermark()

    if last_watermark:
        print(f"Running incremental extract since {last_watermark}")
        df = pd.read_sql(INCREMENTAL_QUERY, conn, params=[last_watermark])
    else:
        print("Running full extract (first run)")
        df = pd.read_sql(BASE_QUERY, conn)

    conn.close()

    if df.empty:
        print("No new data to extract.")
        return

    extract_date = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    raw_file = f"{RAW_DIR}/orders_{extract_date}.parquet"

    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_parquet(raw_file, index=False)

    max_ts = df["updated_at"].max().isoformat()
    write_new_watermark(max_ts)

    print(f"Extracted {len(df)} rows into {raw_file}")
    print(f"Updated watermark to {max_ts}")

if __name__ == "__main__":
    extract()
