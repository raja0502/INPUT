import pandas as pd
import sqlite3
import os

csv_path = "bank-full.csv"   # IMPORTANT: new clean file

# Read normal comma-separated CSV
df = pd.read_csv(csv_path)

print("Loaded rows:", len(df))
print("Columns:", df.columns.tolist())

# Add synthetic primary key
df.insert(0, "customer_id", range(1, len(df) + 1))

db_path = "near_production.db"
if os.path.exists(db_path):
    os.remove(db_path)

conn = sqlite3.connect(db_path)
df.to_sql("customers", conn, if_exists="replace", index=False)
conn.close()

print("Created near_production.db with table 'customers'")
