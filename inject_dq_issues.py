import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

db_path = "near_production.db"
conn = sqlite3.connect(db_path)

df_cust = pd.read_sql_query("SELECT * FROM customers", conn)
df_acc  = pd.read_sql_query("SELECT * FROM accounts", conn)
df_txn  = pd.read_sql_query("SELECT * FROM transactions", conn)

# ---------------- CUSTOMERS ----------------
N = len(df_cust)

# 12% missing job (completeness)
idx = df_cust.sample(frac=0.12, random_state=101).index
df_cust.loc[idx, "job"] = None

# 10% invalid education (validity)
idx = df_cust.sample(frac=0.10, random_state=102).index
df_cust.loc[idx, "education"] = "invalid_edu"

# 8% negative balance (validity/accuracy)
idx = df_cust.sample(frac=0.08, random_state=103).index
df_cust.loc[idx, "balance"] = np.random.uniform(-10000, -10, size=len(idx))

print("Customers issues injected:",
      "null job ~12%, invalid_edu ~10%, negative balance ~8%")

# ---------------- ACCOUNTS ----------------
M = len(df_acc)

# 6% accounts with bad customer_id (integrity)
idx = df_acc.sample(frac=0.06, random_state=201).index
df_acc.loc[idx, "customer_id"] = df_cust["customer_id"].max() + np.random.randint(
    1, 5000, size=len(idx)
)

# 5% future opened_date (timeliness)
if "opened_date" in df_acc.columns:
    idx = df_acc.sample(frac=0.05, random_state=202).index
    future_date = (datetime.now() + timedelta(days=365)).date().isoformat()
    df_acc.loc[idx, "opened_date"] = future_date
    print("Accounts issues injected: bad FK ~6%, future opened_date ~5%")
else:
    print("Accounts issues injected: bad FK ~6% (no opened_date column found)")

# ---------------- TRANSACTIONS ----------------
T = len(df_txn)

# 6% negative amounts
idx = df_txn.sample(frac=0.06, random_state=301).index
df_txn.loc[idx, "amount"] = np.random.uniform(-20000, -10, size=len(idx))

# 5% extreme amounts > 200000
idx = df_txn.sample(frac=0.05, random_state=302).index
df_txn.loc[idx, "amount"] = np.random.uniform(200000, 1000000, size=len(idx))

# 10% NULL channel (completeness)
idx = df_txn.sample(frac=0.10, random_state=303).index
df_txn.loc[idx, "channel"] = None

# 8% invalid status (validity)
idx = df_txn.sample(frac=0.08, random_state=304).index
df_txn.loc[idx, "status"] = "INVALID"

# 4% future txn_date (timeliness)
if "txn_date" in df_txn.columns:
    idx = df_txn.sample(frac=0.04, random_state=305).index
    future_txn_date = (datetime.now() + timedelta(days=180)).date().isoformat()
    df_txn.loc[idx, "txn_date"] = future_txn_date

# 5% broken FK on customer_id (integrity)
idx = df_txn.sample(frac=0.05, random_state=306).index
df_txn.loc[idx, "customer_id"] = df_cust["customer_id"].max() + np.random.randint(
    1, 5000, size=len(idx)
)

print("Transactions issues injected:",
      "neg amount ~6%, extreme amount ~5%, null channel ~10%,",
      "invalid status ~8%, future date ~4%, bad FK ~5%")

# ---------------- SAVE BACK ----------------
df_cust.to_sql("customers", conn, if_exists="replace", index=False)
df_acc.to_sql("accounts", conn, if_exists="replace", index=False)
df_txn.to_sql("transactions", conn, if_exists="replace", index=False)

conn.close()
print("Saved updated customers, accounts, transactions with heavier DQ issues.")
print("Now run T.py again to see the new approximate DQ score (target ≈ 85%).")
