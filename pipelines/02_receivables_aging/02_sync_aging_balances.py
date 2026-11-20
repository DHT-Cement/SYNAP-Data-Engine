# Databricks notebook source
# MAGIC %md
# MAGIC # 02: Dynamics 365 Debtor Aging & Customer Master Ingestion
# MAGIC 
# MAGIC **Source:** `BC_Aging_*.csv/.xlsx` + `BC_Customers_*.csv` + `Customer Master.xlsx`  
# MAGIC **Target Database:** Strictly Development Database (`market_reach` on port `5435`)  
# MAGIC **Target Tables:** `public.aging`, `public.customers`, `public.receivable_history`

# COMMAND ----------
import json
import os
import psycopg2

# COMMAND ----------
# Load Development Configuration
with open("/Workspace/Users/engproject@dhtcement.lk/SYNAP-Data-Engine/config/dev.json", "r") as f:
    cfg = json.load(f)

# STRICT SAFETY CHECK: Verify Target is DEV
assert cfg["database"]["port"] == 5435, "SAFETY ABORT: Target port is NOT 5435 (Dev)!"
assert cfg["env"] == "development", "SAFETY ABORT: Environment is NOT development!"

DB_HOST = cfg["database"]["host"]
DB_PORT = cfg["database"]["port"]
DB_NAME = cfg["database"]["name"]
DB_USER = cfg["database"]["user"]
DB_PASS = "Vibhu2003"

print(f"🔒 SAFETY LOCK VERIFIED: Strictly targeting DEV PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Debtor Aging Transformation & Upsert

# COMMAND ----------
def sync_aging_to_dev_postgres(aging_rows, as_of_date, source_filename):
    """
    aging_rows = [[code, name, balance, current, p1, p2, p3], ...]
    Strictly target: market_reach on port 5435
    """
    if not aging_rows:
        return 0

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=15
    )
    cur = conn.cursor()
    try:
        # Delete old aging records and insert latest snapshot
        cur.execute("DELETE FROM aging;")
        
        insert_vals = []
        total_bal = 0.0
        overdue_bal = 0.0
        for r in aging_rows:
            code, name, bal, cur_amt, p1, p2, p3 = r[0], r[1], float(r[2] or 0), float(r[3] or 0) if r[3] is not None else None, float(r[4] or 0), float(r[5] or 0), float(r[6] or 0)
            if bal > 0:
                total_bal += bal
                if cur_amt is not None:
                    overdue_bal += max(0.0, bal - cur_amt)
            insert_vals.append((code, name, bal, cur_amt, p1, p2, p3, as_of_date, json.dumps([]), source_filename))

        cur.executemany("""
            INSERT INTO aging (code, name, balance, current, p1, p2, p3, as_of, labels, file, at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now());
        """, insert_vals)

        # Update receivable history
        cur.execute("""
            INSERT INTO receivable_history (day, balance, overdue, customers, source)
            VALUES (%s, %s, %s, %s, 'aging')
            ON CONFLICT (day) DO UPDATE SET
                balance = EXCLUDED.balance,
                overdue = EXCLUDED.overdue,
                customers = EXCLUDED.customers,
                source = 'aging';
        """, (as_of_date, round(total_bal), round(overdue_bal), len([r for r in aging_rows if float(r[2] or 0) > 0])))

        conn.commit()
        print(f"✅ AGING SYNC COMPLETE: {len(aging_rows)} debtors (Total Rs. {total_bal/1e6:.1f}Mn, Overdue Rs. {overdue_bal/1e6:.1f}Mn) written to DEV!")
        return len(aging_rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Customer Master Transformation & Upsert

# COMMAND ----------
def sync_customers_to_dev_postgres(customer_list, source_filename="Customer Master.xlsx"):
    """
    customer_list = [{code, name, ship_loc, district, category, l_code, balance, overdue}, ...]
    """
    if not customer_list:
        return 0

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=15
    )
    cur = conn.cursor()
    try:
        cur.execute("UPDATE customers SET in_master = false;")
        vals = []
        for x in customer_list:
            vals.append((
                x["code"], x["name"], x.get("ship_loc", ""), x.get("district", ""),
                x.get("category", ""), x.get("l_code", ""), x.get("balance", 0.0),
                x.get("overdue", 0.0), source_filename
            ))

        cur.executemany("""
            INSERT INTO customers (code, name, ship_loc, district, category, l_code, balance, overdue, from_file, in_master, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, true, now())
            ON CONFLICT (code) DO UPDATE SET
                name = EXCLUDED.name,
                ship_loc = EXCLUDED.ship_loc,
                district = EXCLUDED.district,
                category = EXCLUDED.category,
                l_code = EXCLUDED.l_code,
                balance = EXCLUDED.balance,
                overdue = EXCLUDED.overdue,
                from_file = EXCLUDED.from_file,
                in_master = true,
                updated_at = now();
        """, vals)

        conn.commit()
        print(f"✅ CUSTOMERS SYNC COMPLETE: {len(customer_list)} customers updated in DEV PostgreSQL!")
        return len(customer_list)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

print("Pipeline 02: Debtor Aging & Customer Master ready and validated against Dev PostgreSQL!")
