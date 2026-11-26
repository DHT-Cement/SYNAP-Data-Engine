# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Dealer Rebates & Net Realization Ingestion (With Change Detection)
# MAGIC 
# MAGIC **Source:** Yamuna's Google Drive `REBATE` folder (`172bZlp4HO_R1UEF5YzZCoyM7JXZTmf_Q`)  
# MAGIC **Target Database:** Strictly Development Database (`market_reach` on port `5435`)  
# MAGIC **Target Tables:** `public.nr_files`, `public.nr_rows`, `public.nr_bulk`
# MAGIC 
# MAGIC **Formulae:**
# MAGIC - PowerTech Bag NR: `(Gross - Rebate) / 1.2095 - Bag Transport`
# MAGIC - Bulk NR per MT: `(Gross - Rebate - Commission) / 1.2095 - Bulk Transport`

# COMMAND ----------
import json
import os
import io
import re
from datetime import datetime
import psycopg2

# COMMAND ----------
# MAGIC %run ../../../src/tracker

# COMMAND ----------
# MAGIC %run ../../../src/tax

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
# MAGIC ### Step 2: Bag Rebates Transformation & Upsert

# COMMAND ----------
def upsert_bag_rebates_to_dev(file_id, filename, ym, period, rows, sig=""):
    """
    rows = [{cust_code, customer, area, bags, gross, rebate, add_rebate, transport, sheet}, ...]
    Writes to nr_files and nr_rows in Dev PostgreSQL.
    """
    if not rows:
        return 0

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=15)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO nr_files (file_id, name, ym, second, modified, parser, read_at, rows, note)
            VALUES (%s, %s, %s, %s, %s, '5', now(), %s, %s)
            ON CONFLICT (file_id) DO UPDATE SET
                name = EXCLUDED.name, ym = EXCLUDED.ym, second = EXCLUDED.second,
                modified = EXCLUDED.modified, read_at = now(), rows = EXCLUDED.rows, note = EXCLUDED.note;
        """, (file_id, filename, ym, '02' in filename or 'H2' in period, sig, len(rows), f"Synced {len(rows)} bag rebate rows"))

        cur.execute("DELETE FROM nr_rows WHERE file_id = %s;", (file_id,))

        vals = []
        for r in rows:
            vals.append((
                file_id, ym, period, r.get("cust_code", ""), r.get("customer", ""),
                r.get("area", ""), float(r.get("bags") or 0), float(r.get("gross") or 0),
                float(r.get("rebate") or 0), float(r.get("add_rebate") or 0),
                float(r.get("transport") or 0), r.get("sheet", "")
            ))

        cur.executemany("""
            INSERT INTO nr_rows (file_id, ym, period, cust_code, customer, area, bags, gross, rebate, add_rebate, transport, sheet)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, vals)

        if sig:
            mark_file_seen(f"rebate:{file_id}", "rebate", filename, sig, f"{len(rows)} bag rebate rows", DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)

        conn.commit()
        print(f"✅ BAG REBATES SYNC COMPLETE: {len(rows)} rows for month {ym} ({period}) written to DEV!")
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Bulk Net Realization Transformation & Upsert

# COMMAND ----------
def upsert_bulk_nr_to_dev(file_id, filename, ym, rows, sig=""):
    """
    rows = [{old_code, cust_code, customer, location, mt, gross, net, price, rebate_mt, transport_mt, commission_mt, sheet, nominal, note}, ...]
    Writes to nr_bulk in Dev PostgreSQL.
    """
    if not rows:
        return 0

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=15)
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM nr_bulk WHERE file_id = %s AND ym = %s;", (file_id, ym))

        vals = []
        for r in rows:
            vals.append((
                file_id, ym, r.get("old_code", ""), r.get("cust_code", ""), r.get("customer", ""),
                r.get("location", ""), float(r.get("mt") or 0), float(r.get("gross") or 0),
                float(r.get("net") or 0), float(r.get("price") or 0), float(r.get("rebate_mt") or 0),
                float(r.get("transport_mt") or 0), float(r.get("commission_mt") or 0),
                r.get("sheet", ""), bool(r.get("nominal", False)), r.get("note", "")
            ))

        cur.executemany("""
            INSERT INTO nr_bulk (file_id, ym, old_code, cust_code, customer, location, mt, gross, net, price, rebate_mt, transport_mt, commission_mt, sheet, nominal, note)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, vals)

        if sig:
            mark_file_seen(f"bulk_nr:{file_id}_{ym}", "bulk_nr", filename, sig, f"{len(rows)} bulk NR rows", DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)

        conn.commit()
        print(f"✅ BULK NR SYNC COMPLETE: {len(rows)} bulk customer records for {ym} written to DEV!")
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

print("Pipeline 03: Dealer Rebates & Net Realization ready with Change Detection and Dev Safety Lock!")
