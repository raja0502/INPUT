import sqlite3
import pandas as pd

db_path = "near_production.db"

tables = [
    "customers_raw",
    "accounts_raw",
    "transactions_raw",
    "customers",
    "accounts",
    "transactions",
]

conn = sqlite3.connect(db_path)

for t in tables:
    try:
        df = pd.read_sql_query(f"SELECT * FROM {t}", conn)
        csv_name = f"{t}.csv"
        df.to_csv(csv_name, index=False)
        print(f"Exported {t} -> {csv_name} (rows={len(df)})")
    except Exception as e:
        print(f"Skipping {t}: {e}")

conn.close()
