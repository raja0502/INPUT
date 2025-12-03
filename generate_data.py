import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

db_path = "near_production.db"
conn = sqlite3.connect(db_path)

# Load customers
df_cust = pd.read_sql_query("SELECT customer_id, age, job, marital, education, balance FROM customers", conn)
print("Customers:", len(df_cust))

# ---------- ACCOUNTS ----------
accounts = []
acc_id = 1
for _, row in df_cust.iterrows():
    n_accounts = np.random.choice([1, 1, 1, 2, 3])  # most have 1, few have 2 or 3
    for _ in range(n_accounts):
        account_type = random.choice(["SAVINGS", "CURRENT", "LOAN"])
        days_ago = int(np.random.exponential(scale=700)) % 2000
        opened_date = (datetime.now() - timedelta(days=days_ago)).date().isoformat()
        status = random.choices(["ACTIVE", "CLOSED"], weights=[0.85, 0.15])[0]
        accounts.append({
            "account_id": acc_id,
            "customer_id": int(row["customer_id"]),
            "account_type": account_type,
            "opened_date": opened_date,
            "status": status
        })
        acc_id += 1

df_acc = pd.DataFrame(accounts)
print("Accounts:", len(df_acc))
df_acc.to_sql("accounts_raw", conn, if_exists="replace", index=False)
df_acc.to_sql("accounts", conn, if_exists="replace", index=False)

# ---------- TRANSACTIONS ----------
transactions = []
txn_id = 1
account_ids = df_acc["account_id"].tolist()

for _ in range(300000):   # 3 lakh txns
    account_id = random.choice(account_ids)
    acc_row = df_acc[df_acc["account_id"] == account_id].iloc[0]
    cust_id = int(acc_row["customer_id"])
    # amount depends on account type
    if acc_row["account_type"] == "LOAN":
        base = np.random.normal(10000, 5000)
    else:
        base = np.random.normal(2000, 1000)
    amount = max(50, round(abs(base), 2))

    days_ago = int(np.random.exponential(scale=150)) % 365
    txn_date = (datetime.now() - timedelta(days=days_ago)).date().isoformat()

    channel = random.choice(["ONLINE", "BRANCH", "ATM", "UPI", "CARD"])
    category = random.choice(["BILL", "SHOPPING", "SALARY", "EMI", "FEE", "TRANSFER"])
    status = random.choices(["SUCCESS", "FAILED", "PENDING"], weights=[0.9, 0.06, 0.04])[0]

    transactions.append({
        "txn_id": txn_id,
        "account_id": account_id,
        "customer_id": cust_id,
        "txn_date": txn_date,
        "amount": amount,
        "channel": channel,
        "category": category,
        "status": status
    })
    txn_id += 1

df_txn = pd.DataFrame(transactions)
print("Transactions:", len(df_txn))
df_txn.to_sql("transactions_raw", conn, if_exists="replace", index=False)
df_txn.to_sql("transactions", conn, if_exists="replace", index=False)

conn.close()
print("accounts_raw/accounts and transactions_raw/transactions saved into near_production.db")
