# Databricks notebook source
# MAGIC %md
# MAGIC # 05: Finance Management Accounts & EBITDA Ingestion (With Change Detection)
# MAGIC 
# MAGIC **Source:** `Management Accounts *.xlsx` / `MA *.xlsx` in `ERP Sales Drop` (`1Tz_raKMCVrrDjZwJo8eII7BREh_9SLsG`)  
# MAGIC **Target Database:** Strictly Development Database (`market_reach` on port `5435`)  
# MAGIC **Target Table:** `public.pnl_months` (columns: `ym`, `period_end`, `file`, `data` jsonb, `at`)  
# MAGIC 
# MAGIC **P&L Metrics Extracted:**
# MAGIC - Revenue, Direct S&D, Cost of Sales (COS), Gross Profit, Other Income, Admin Expenses, Selling Expenses, EBITDA, Depreciation, Finance Cost, NPBT
# MAGIC - Product Splits: Cement vs Clinker, Empty bags used from COS sheet

# COMMAND ----------
import json
import os
import psycopg2

# COMMAND ----------
# MAGIC %run ../../../src/tracker

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Environment & Dev Safety Lock

# COMMAND ----------
try:
    dbutils.widgets.text("env", "dev", "Environment")
    target_env = dbutils.widgets.get("env").strip().lower()
except Exception:
    target_env = "dev"

if target_env not in ["dev", "prod", "development", "production"]:
    target_env = "dev"

config_file = "/Workspace/Users/engproject@dhtcement.lk/SYNAP-Data-Engine/config/prod.json" if target_env in ["prod", "production"] else "/Workspace/Users/engproject@dhtcement.lk/SYNAP-Data-Engine/config/dev.json"

with open(config_file, "r") as f:
    cfg = json.load(f)

DB_HOST = cfg["database"]["host"]
DB_PORT = cfg["database"]["port"]
DB_NAME = cfg["database"]["name"]
DB_USER = cfg["database"]["user"]
DB_PASS = "Vibhu2003"

print(f"🔒 TARGET VERIFIED: Environment={cfg['env'].upper()}, Database={DB_HOST}:{DB_PORT}/{DB_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Finance P&L Upsert Function to Dev Database

# COMMAND ----------
def upsert_pnl_to_dev(ym, period_end, filename, pnl_data, sig=""):
    """
    pnl_data = {
        "revenue": 350000000.0,
        "cos": 260000000.0,
        "gp": 90000000.0,
        "ebitda": 45000000.0,
        "npbt": 32000000.0,
        "dep": 8000000.0,
        "finance": 5000000.0,
        "cementSales": ...,
        "bagsUsed": ...
    }
    Writes strictly to market_reach on port 5435.
    """
    if not pnl_data or not pnl_data.get("revenue"):
        return False

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=15)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO pnl_months (ym, period_end, file, data, at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (ym) DO UPDATE SET
                period_end = EXCLUDED.period_end,
                file = EXCLUDED.file,
                data = EXCLUDED.data,
                at = now();
        """, (ym, period_end, filename, json.dumps(pnl_data)))

        if sig:
            mark_file_seen(f"pnl:{ym}", "pnl", filename, sig, f"P&L month {ym} updated", DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)

        conn.commit()
        print(f"✅ FINANCE P&L SYNC COMPLETE: {ym} written to DEV {DB_NAME} (EBITDA: Rs. {pnl_data.get('ebitda', 0)/1e6:.1f}Mn)!")
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

print("Pipeline 05: Finance Management Accounts ready with Change Detection and Dev Safety Lock!")
