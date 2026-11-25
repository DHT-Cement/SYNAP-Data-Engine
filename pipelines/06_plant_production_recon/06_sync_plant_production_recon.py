# Databricks notebook source
# MAGIC %md
# MAGIC # 06: Plant Daily Production & ERP Reconciliation Pipeline (With Real-Time Change Detection)
# MAGIC 
# MAGIC - Scans Google Drive `Plant Production Reports` folder (`1lLhkSI0w11V8lRAJMtLEobQvTkoe8h-C`) for daily dispatch reports.
# MAGIC - Extracts physical plant dispatches (Bulk, Powertech, Saviya).
# MAGIC - Compares with ERP dispatches from `erp_days` table.
# MAGIC - Reconciles and writes to PostgreSQL tables `public.production_reports` and `public.recon`.

# COMMAND ----------
# MAGIC %pip install google-api-python-client google-auth psycopg2-binary openpyxl --quiet

# COMMAND ----------
import json
import os
import io
import re
from datetime import datetime
import psycopg2
import openpyxl
from googleapiclient.discovery import build
from google.oauth2 import service_account

# COMMAND ----------
# MAGIC %run ../../src/tracker

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Environment & Config Lock

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
PLANT_FOLDER_ID = "1lLhkSI0w11V8lRAJMtLEobQvTkoe8h-C"
TOLERANCE_MT = 0.05

print(f"🔒 TARGET VERIFIED: Environment={target_env.upper()}, Database={DB_HOST}:{DB_PORT}/{DB_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Google Drive Scan & Plant Production Upsert

# COMMAND ----------
SERVICE_ACCOUNT_PATH = "/Workspace/Users/engproject@dhtcement.lk/SYNAP-Data-Engine/config/service_account.json"
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

print(f"Connecting to Google Drive with Service Account: {SERVICE_ACCOUNT_PATH}...")
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

res = drive_service.files().list(
    q=f"'{PLANT_FOLDER_ID}' in parents and trashed = false",
    fields="files(id, name, modifiedTime, size)",
    pageSize=30
).execute()
plant_files = res.get('files', [])
print(f"Found {len(plant_files)} files in Plant Production folder.")

report_files = [f for f in plant_files if re.search(r"(\d{4}[.-]\d{2}[.-]\d{2})", f['name']) and f['name'].endswith(('.xlsx', '.xlsm', '.xls'))]
report_files.sort(key=lambda x: x['name'], reverse=True)

conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=15)
cur = conn.cursor()

try:
    for rf in report_files:
        m = re.search(r"(\d{4})[.-](\d{2})[.-](\d{2})", rf['name'])
        if not m: continue
        day_iso = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        
        rf_mod = int(datetime.fromisoformat(rf['modifiedTime'].replace('Z', '+00:00')).timestamp() * 1000)
        sig = f"{rf['name']}@{rf_mod}"
        
        file_id = f"prod:{day_iso}"
        if not has_file_changed(file_id, sig, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS):
            continue
            
        print(f"📥 [SYNC] Processing Plant Production Report {rf['name']} for {day_iso}...")
        try:
            content_bytes = drive_service.files().get_media(fileId=rf['id']).execute()
            wb = openpyxl.load_workbook(io.BytesIO(content_bytes), data_only=True)
            
            bulk_mt = 0.0
            pt_mt = 0.0
            sv_mt = 0.0
            prod_mt = 0.0
            
            for sheetname in wb.sheetnames:
                ws = wb[sheetname]
                for r in ws.iter_rows(values_only=True):
                    for idx, cell in enumerate(r):
                        val_str = str(cell or '').strip().lower()
                        if 'bulk' in val_str and idx + 1 < len(r) and isinstance(r[idx+1], (int, float)):
                            bulk_mt = max(bulk_mt, float(r[idx+1]))
                        elif 'powertech' in val_str and idx + 1 < len(r) and isinstance(r[idx+1], (int, float)):
                            pt_mt = max(pt_mt, float(r[idx+1]))
                        elif 'saviya' in val_str and idx + 1 < len(r) and isinstance(r[idx+1], (int, float)):
                            sv_mt = max(sv_mt, float(r[idx+1]))
                        elif 'production' in val_str and idx + 1 < len(r) and isinstance(r[idx+1], (int, float)):
                            prod_mt = max(prod_mt, float(r[idx+1]))
            
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
                    source = 'email';
            """, (day_iso, rf['name'], bulk_mt, pt_mt, sv_mt, prod_mt, json.dumps({})))
            
            mark_file_seen(file_id, "production", rf['name'], sig, f"Plant report {day_iso} updated", DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
            print(f"✅ Plant report saved for {day_iso}: Bulk={bulk_mt}, PT={pt_mt}, SV={sv_mt}")
        except Exception as ex:
            print(f"Warning: Could not parse {rf['name']}: {ex}")

    # Step 3: Run Daily Reconciliation Engine for All Days
    cur.execute("SELECT day, mt, rs FROM erp_days WHERE day >= '2026-11-01' ORDER BY day;")
    erp_rows = cur.fetchall()
    
    for erow in erp_rows:
        day_iso = str(erow[0])[:10]
        raw_mt = erow[1]
        erp_mt = raw_mt if isinstance(raw_mt, dict) else (json.loads(raw_mt) if raw_mt else {"Bulk": 0.0, "Powertech": 0.0, "Saviya": 0.0})
        raw_rs = erow[2]
        erp_rs = raw_rs if isinstance(raw_rs, dict) else (json.loads(raw_rs) if raw_rs else {"Bulk": 0.0, "Powertech": 0.0, "Saviya": 0.0})
        total_rs = sum(erp_rs.values())
        
        cur.execute("SELECT bulk_mt, pt_mt, sv_mt, production, file FROM production_reports WHERE day = %s;", (day_iso,))
        prod_row = cur.fetchone()
        
        if not prod_row:
            cur.execute("""
                INSERT INTO recon (day, status, erp, rs, details, updated_at)
                VALUES (%s, 'WAITING', %s, %s, 'Waiting for daily production report', now())
                ON CONFLICT (day) DO UPDATE SET status = 'WAITING', updated_at = now();
            """, (day_iso, json.dumps(erp_mt), total_rs))
            continue
            
        prod_mt = {"Bulk": float(prod_row[0] or 0), "Powertech": float(prod_row[1] or 0), "Saviya": float(prod_row[2] or 0)}
        prod_total_mt = float(prod_row[3] or 0)
        report_file = prod_row[4]
        
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

        cur.execute("""
            INSERT INTO recon (day, status, erp, prod, rs, production, details, report, updated_at, ever_posted)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (day) DO UPDATE SET
                status = CASE WHEN recon.status = 'APPROVED' THEN 'APPROVED' ELSE EXCLUDED.status END,
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
    print("✅ Reconciliation complete for all days!")
finally:
    cur.close()
    conn.close()

print("🎉 Pipeline 06 Complete!")
