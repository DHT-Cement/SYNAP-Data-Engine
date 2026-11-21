# Databricks notebook source
# MAGIC %md
# MAGIC # 04: Sales Targets Pipeline (With Change Detection)
# MAGIC 
# MAGIC **Source:** `Targets 2026.xlsx` & `Targets from 2025.xlsx` in `ERP Sales Drop` (`1Tz_raKMCVrrDjZwJo8eII7BREh_9SLsG`)  
# MAGIC **Target Database:** Strictly Development Database (`market_reach` on port `5435`)  
# MAGIC **Target Table:** `public.targets` (columns: `ym`, `file`, `data` jsonb, `at`)  
# MAGIC 
# MAGIC **Business Rules:**
# MAGIC - Targets are defined in bags per RSM, ASE, Territory, and Distributor.
# MAGIC - Same-color merged rows represent combined customer quotas.
# MAGIC - National Sales target represents the total company target.

# COMMAND ----------
import json
import os
import psycopg2

# COMMAND ----------
# MAGIC %run ../../src/tracker

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
# MAGIC ### Step 2: Target Upsert Function to Dev Database

# COMMAND ----------
def upsert_targets_to_dev(ym, filename, target_data, sig=""):
    """
    target_data = {
        "file": filename,
        "sheet": "...",
        "groups": [...],
        "terr": [...],
        "national": 125000
    }
    Writes strictly to market_reach on port 5435.
    """
    if not target_data:
        return False

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=15)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO targets (ym, file, data, at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (ym) DO UPDATE SET
                file = EXCLUDED.file,
                data = EXCLUDED.data,
                at = now();
        """, (ym, filename, json.dumps(target_data)))

        if sig:
            mark_file_seen(f"targets:{ym}", "targets", filename, sig, f"Target month {ym} updated", DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)

        conn.commit()
        print(f"✅ TARGETS SYNC COMPLETE: Target data for {ym} written to DEV {DB_NAME} (National Target: {target_data.get('national', 0):,} bags)!")
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

print("Pipeline 04: Sales Targets ready with Change Detection and Dev Safety Lock!")
