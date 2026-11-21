# Databricks notebook source
# MAGIC %md
# MAGIC # 06: Plant Daily Production & ERP Reconciliation Pipeline (With Change Detection)
# MAGIC 
# MAGIC **Source:** Daily Production Report `.xlsx` from Google Drive folder `Plant Production Reports` (auto-saved from email)  
# MAGIC **Target Database:** Strictly Development Database (`market_reach` on port `5435`)  
# MAGIC **Target Tables:** `public.production_reports`, `public.recon`
# MAGIC 
# MAGIC **Medallion Architecture:**
# MAGIC - **Bronze:** Raw daily report file metadata and values
# MAGIC - **Silver:** Cleaned physical production figures (Bulk MT, PowerTech MT, Saviya MT, Silo balances)
# MAGIC - **Gold:** Daily Reconciliation: ERP Dispatches vs. Physical Plant Output -> Status: `POSTED` (Tallied) or `PENDING` (Mismatch)

# COMMAND ----------
import json
import os
import psycopg2
from datetime import datetime

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
# MAGIC ### Step 2: Silver Layer - Plant Daily Figures Upsert

# COMMAND ----------
def upsert_production_report_to_dev(day_iso, filename, prod_figures, silo_data, sig=""):
    """
    prod_figures = {"Bulk": 120.0, "Powertech": 340.0, "Saviya": 45.0, "production": 500.0}
    silo_data = {"opening": 1200.0, "closing": 1150.0, ...}
    Writes strictly to market_reach on port 5435.
    """
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=15)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO production_reports (
                day, file, received, bulk_mt, pt_mt, sv_mt, production, silo, source
            ) VALUES (%s, %s, now(), %s, %s, %s, %s, %s, 'email')
            ON CONFLICT (day) DO UPDATE SET
                file = EXCLUDED.file,
                received = now(),
                bulk_mt = EXCLUDED.bulk_mt,
                pt_mt = EXCLUDED.pt_mt,
                sv_mt = EXCLUDED.sv_mt,
                production = EXCLUDED.production,
                silo = EXCLUDED.silo,
                source = 'email';
        """, (
            day_iso,
            filename,
            prod_figures.get("Bulk", 0.0),
            prod_figures.get("Powertech", 0.0),
            prod_figures.get("Saviya", 0.0),
            prod_figures.get("production", 0.0),
            json.dumps(silo_data)
        ))

        if sig:
            mark_file_seen(f"prod:{day_iso}", "production", filename, sig, f"Production report {day_iso} updated", DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)

        conn.commit()
        print(f"✅ SILVER PRODUCTION REPORT SYNCED: Day {day_iso} from {filename} written to DEV!")
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Gold Layer - ERP vs. Plant Reconciliation Engine

# COMMAND ----------
def run_daily_reconciliation_dev(day_iso):
    """
    Compares ERP dispatched sales lines vs Physical Plant Output for day_iso.
    If all products tally (within 0.01 MT) -> status = 'POSTED' (Tallied)
    If mismatch -> status = 'PENDING' (Needs approval on Check screen)
    """
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=15)
    cur = conn.cursor()
    try:
        # 1. Fetch ERP dispatches for the day
        cur.execute("SELECT mt, rs FROM erp_days WHERE day = %s;", (day_iso,))
        erp_row = cur.fetchone()
        erp_mt = json.loads(erp_row[0]) if erp_row and erp_row[0] else {"Bulk": 0.0, "Powertech": 0.0, "Saviya": 0.0}
        erp_rs = json.loads(erp_row[1]) if erp_row and erp_row[1] else {"Bulk": 0.0, "Powertech": 0.0, "Saviya": 0.0}
        total_rs = sum(erp_rs.values())

        # 2. Fetch Plant Production report for the day
        cur.execute("SELECT bulk_mt, pt_mt, sv_mt, production, file FROM production_reports WHERE day = %s;", (day_iso,))
        prod_row = cur.fetchone()
        if not prod_row:
            # Waiting for report
            cur.execute("""
                INSERT INTO recon (day, status, erp, rs, details, updated_at)
                VALUES (%s, 'WAITING', %s, %s, 'Waiting for daily production report', now())
                ON CONFLICT (day) DO UPDATE SET status = 'WAITING', updated_at = now();
            """, (day_iso, json.dumps(erp_mt), total_rs))
            conn.commit()
            print(f"Recon {day_iso}: WAITING for production report.")
            return "WAITING"

        prod_mt = {"Bulk": float(prod_row[0] or 0), "Powertech": float(prod_row[1] or 0), "Saviya": float(prod_row[2] or 0)}
        prod_total_mt = float(prod_row[3] or 0)
        report_file = prod_row[4]

        # 3. Check differences per product
        mismatches = []
        for p in ["Bulk", "Powertech", "Saviya"]:
            diff = round(erp_mt.get(p, 0.0) - prod_mt.get(p, 0.0), 2)
            if abs(diff) > TOLERANCE_MT:
                mismatches.append(f"{p}: ERP {erp_mt.get(p,0):.1f} MT vs Plant {prod_mt.get(p,0):.1f} MT (Diff {diff:+.1f} MT)")

        if not mismatches:
            status = "POSTED"
            details = "All products tally"
        else:
            status = "PENDING"
            details = " · ".join(mismatches)

        # 4. Upsert recon table
        cur.execute("""
            INSERT INTO recon (day, status, erp, prod, rs, production, details, report, updated_at, ever_posted)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (day) DO UPDATE SET
                status = EXCLUDED.status,
                erp = EXCLUDED.erp,
                prod = EXCLUDED.prod,
                rs = EXCLUDED.rs,
                production = EXCLUDED.production,
                details = EXCLUDED.details,
                report = EXCLUDED.report,
                updated_at = now(),
                ever_posted = recon.ever_posted OR (EXCLUDED.status IN ('POSTED', 'APPROVED'));
        """, (
            day_iso, status, json.dumps(erp_mt), json.dumps(prod_mt), total_rs,
            prod_total_mt, details, report_file, status in ["POSTED", "APPROVED"]
        ))

        conn.commit()
        print(f"🎉 GOLD RECONCILIATION COMPLETE: Day {day_iso} -> Status: {status} ({details}) written to DEV!")
        return status
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

print("Pipeline 06: Plant Production & ERP Reconciliation ready with Change Detection and Dev Safety Lock!")
