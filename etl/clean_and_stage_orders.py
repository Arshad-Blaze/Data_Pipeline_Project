import pandas as pd
from datetime import datetime
import duckdb
import os

RAW_FILE = sorted(os.listdir("data/raw"))[-1]
RAW_PATH = f"data/raw/{RAW_FILE}"
STAGING_PATH = "data/staging/orders.parquet"
DB_PATH = "warehouse/warehouse.duckdb"

df = pd.read_parquet(RAW_PATH)

df = df.dropna(subset=["order_id"])
df["updated_at"] = pd.to_datetime(df["updated_at"])

df = (
    df.sort_values("updated_at", ascending=False)
      .drop_duplicates(subset=["order_id"], keep="first")
)

df["ingestion_date"] = datetime.utcnow().date()

os.makedirs("data/staging", exist_ok=True)
df.to_parquet(STAGING_PATH, index=False)

con = duckdb.connect(DB_PATH)
con.execute("""
    CREATE OR REPLACE TABLE stg_orders AS
    SELECT * FROM read_parquet(?)
""", [STAGING_PATH])

con.close()

print("Staging completed successfully")
