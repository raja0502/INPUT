import sqlite3
import pandas as pd

conn = sqlite3.connect("near_production.db")
df_cust = pd.read_sql_query("SELECT * FROM customers", conn)
df_cust.to_sql("customers_raw", conn, if_exists="replace", index=False)
conn.close()
