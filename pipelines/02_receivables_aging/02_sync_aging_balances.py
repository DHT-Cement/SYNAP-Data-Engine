# Databricks notebook source
# MAGIC %md
# MAGIC # 02: Dynamics 365 Debtor Aging & Customer Master Ingestion (With Real-Time Change Detection)
# MAGIC 
# MAGIC - Scans Google Drive `ERP Sales Drop` folder (`1Tz_raKMCVrrDjZwJo8eII7BREh_9SLsG`) for `BC_Aging_*.csv` and `Customer Master.xlsx`.
# MAGIC - **Change Detection:** Computes signature (`filename @ modifiedTime`). If no changes, exits in seconds.
# MAGIC - Writes to PostgreSQL tables `public.aging`, `public.customers`, and `public.receivable_history`.

# COMMAND ----------
# MAGIC %pip install google-api-python-client google-auth psycopg2-binary openpyxl --quiet

# COMMAND ----------
import json
import os
import io
import csv
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
ERP_FOLDER_ID = cfg.get("drive_folders", {}).get("erp_sales_drop", "1Tz_raKMCVrrDjZwJo8eII7BREh_9SLsG")

print(f"🔒 TARGET VERIFIED: Environment={target_env.upper()}, Database={DB_HOST}:{DB_PORT}/{DB_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Google Drive Connection

# COMMAND ----------
SERVICE_ACCOUNT_PATH = "/Workspace/Users/engproject@dhtcement.lk/SYNAP-Data-Engine/config/service_account.json"
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

print(f"Connecting to Google Drive with Service Account: {SERVICE_ACCOUNT_PATH}...")
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

res = drive_service.files().list(
    q=f"'{ERP_FOLDER_ID}' in parents and trashed = false",
    fields="files(id, name, modifiedTime, size)",
    pageSize=50
).execute()
drive_files = res.get('files', [])
print(f"Found {len(drive_files)} files in folder '{ERP_FOLDER_ID}'.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Debtor Aging Sync

# COMMAND ----------
aging_files = [f for f in drive_files if re.match(r"^BC_Aging_\d{4}-\d{2}-\d{2}", f['name'])]
aging_files.sort(key=lambda x: x['name'], reverse=True)

if not aging_files:
    print("No BC_Aging files found.")
else:
    latest_aging = aging_files[0]
    aging_mod = int(datetime.fromisoformat(latest_aging['modifiedTime'].replace('Z', '+00:00')).timestamp() * 1000)
    sig_aging = f"{latest_aging['name']}@{aging_mod}"
    
    if not has_file_changed("aging", sig_aging, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS):
        print(f"⏩ [UNCHANGED] Debtor Aging: {sig_aging}")
    else:
        print(f"🚀 [PROCESSING] Debtor Aging file {latest_aging['name']}...")
        aging_text = drive_service.files().get_media(fileId=latest_aging['id']).execute().decode('utf-8-sig', errors='replace')
        lines = list(csv.reader(io.StringIO(aging_text)))
        
        if len(lines) > 1:
            header = [c.strip().lower() for c in lines[0]]
            def get_idx(*cols):
                for col in cols:
                    if col.lower() in header: return header.index(col.lower())
                return -1
            
            c_code = get_idx('customernumber', 'customer number', 'no', 'number')
            c_name = get_idx('name', 'customer name', 'displayname')
            c_bal = get_idx('balancedue', 'balance due', 'balance')
            c_cur = get_idx('currentamount', 'current amount', 'current')
            c_p1 = get_idx('period1amount', 'period 1 amount')
            c_p2 = get_idx('period2amount', 'period 2 amount')
            c_p3 = get_idx('period3amount', 'period 3 amount')
            c_asof = get_idx('agedasofdate', 'as of date', 'aged as of date')
            
            m_date = re.search(r"(\d{4}-\d{2}-\d{2})", latest_aging['name'])
            as_of_date = m_date.group(1) if m_date else datetime.now().strftime("%Y-%m-%d")
            
            aging_rows = []
            seen_codes = set()
            total_bal = 0.0
            total_overdue = 0.0
            
            for r in lines[1:]:
                if len(r) <= max(c_code, c_bal): continue
                code = r[c_code].strip().upper()
                if not code or code in seen_codes: continue
                seen_codes.add(code)
                
                name = r[c_name].strip() if c_name >= 0 and len(r) > c_name else ""
                try: bal = float(r[c_bal]) if r[c_bal] else 0.0
                except ValueError: bal = 0.0
                try: cur_amt = float(r[c_cur]) if c_cur >= 0 and len(r) > c_cur and r[c_cur] else 0.0
                except ValueError: cur_amt = 0.0
                try: p1 = float(r[c_p1]) if c_p1 >= 0 and len(r) > c_p1 and r[c_p1] else 0.0
                except ValueError: p1 = 0.0
                try: p2 = float(r[c_p2]) if c_p2 >= 0 and len(r) > c_p2 and r[c_p2] else 0.0
                except ValueError: p2 = 0.0
                try: p3 = float(r[c_p3]) if c_p3 >= 0 and len(r) > c_p3 and r[c_p3] else 0.0
                except ValueError: p3 = 0.0
                
                if c_asof >= 0 and len(r) > c_asof and re.match(r"^\d{4}-\d{2}-\d{2}$", r[c_asof].strip()[:10]):
                    as_of_date = r[c_asof].strip()[:10]
                
                aging_rows.append((code, name, bal, cur_amt, p1, p2, p3, as_of_date, json.dumps([]), latest_aging['name']))
                if bal > 0:
                    total_bal += bal
                    total_overdue += max(0.0, bal - cur_amt)
            
            conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=15)
            cur = conn.cursor()
            try:
                cur.execute("DELETE FROM aging;")
                cur.executemany("""
                    INSERT INTO aging (code, name, balance, current, p1, p2, p3, as_of, labels, file)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, aging_rows)
                
                cur.execute("""
                    INSERT INTO receivable_history (day, balance, overdue, customers, source)
                    VALUES (%s, %s, %s, %s, 'aging')
                    ON CONFLICT (day) DO UPDATE SET
                        balance = EXCLUDED.balance, overdue = EXCLUDED.overdue, customers = EXCLUDED.customers, source = 'aging';
                """, (as_of_date, round(total_bal), round(total_overdue), len(seen_codes)))
                
                mark_file_seen("aging", "aging", latest_aging['name'], sig_aging, f"{len(aging_rows)} customers, Rs {round(total_bal):,} due", DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
                conn.commit()
                print(f"✅ AGING COMPLETE: {len(aging_rows)} customers synced for as_of_date {as_of_date} (Rs {total_bal/1e6:.1f} Mn due)!")
            finally:
                cur.close()
                conn.close()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Customer Master Sync

# COMMAND ----------
cust_files = [f for f in drive_files if 'customer' in f['name'].lower() and f['name'].endswith(('.xlsx', '.xlsm', '.csv'))]
cust_files.sort(key=lambda x: x['modifiedTime'], reverse=True)

if not cust_files:
    print("No Customer Master files found.")
else:
    latest_cust = cust_files[0]
    cust_mod = int(datetime.fromisoformat(latest_cust['modifiedTime'].replace('Z', '+00:00')).timestamp() * 1000)
    sig_cust = f"{latest_cust['name']}@{cust_mod}"
    
    if not has_file_changed("customers", sig_cust, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS):
        print(f"⏩ [UNCHANGED] Customer Master: {sig_cust}")
    else:
        print(f"🚀 [PROCESSING] Customer Master file {latest_cust['name']}...")
        cust_bytes = drive_service.files().get_media(fileId=latest_cust['id']).execute()
        wb = openpyxl.load_workbook(io.BytesIO(cust_bytes), data_only=True)
        sheet = wb.active
        rows = list(sheet.iter_rows(values_only=True))
        
        if len(rows) > 1:
            header = [str(c or '').strip().lower() for c in rows[0]]
            def get_c_idx(*cols):
                for col in cols:
                    for i, h in enumerate(header):
                        if col.lower() in h: return i
                return -1
            
            idx_code = get_c_idx('no.', 'no', 'customer number', 'code')
            idx_name = get_c_idx('name')
            idx_loc = get_c_idx('shipping location', 'location')
            idx_lcode = get_c_idx('l-code', 'l code')
            idx_cat = get_c_idx('customer category', 'category')
            
            customer_records = []
            seen_c = set()
            for r in rows[1:]:
                if len(r) <= idx_code: continue
                c_no = str(r[idx_code] or '').strip().upper()
                if not c_no or c_no in seen_c: continue
                seen_c.add(c_no)
                
                c_name = str(r[idx_name] or '').strip() if idx_name >= 0 and len(r) > idx_name and r[idx_name] else ""
                c_loc = str(r[idx_loc] or '').strip() if idx_loc >= 0 and len(r) > idx_loc and r[idx_loc] else ""
                c_lcode = str(r[idx_lcode] or '').strip() if idx_lcode >= 0 and len(r) > idx_lcode and r[idx_lcode] else ""
                c_cat = str(r[idx_cat] or '').strip() if idx_cat >= 0 and len(r) > idx_cat and r[idx_cat] else ""
                
                customer_records.append((c_no, c_name, c_loc, c_cat, c_lcode, latest_cust['name']))
            
            conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=15)
            cur = conn.cursor()
            try:
                cur.execute("UPDATE customers SET in_master = false;")
                cur.executemany("""
                    INSERT INTO customers (code, name, ship_loc, category, l_code, from_file, in_master, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, true, now())
                    ON CONFLICT (code) DO UPDATE SET
                        name = EXCLUDED.name,
                        ship_loc = EXCLUDED.ship_loc,
                        category = EXCLUDED.category,
                        l_code = EXCLUDED.l_code,
                        from_file = EXCLUDED.from_file,
                        in_master = true,
                        updated_at = now();
                """, customer_records)
                
                mark_file_seen("customers", "customers", latest_cust['name'], sig_cust, f"{len(customer_records)} customers updated", DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
                conn.commit()
                print(f"✅ CUSTOMERS COMPLETE: {len(customer_records)} customers updated in {target_env.upper()} DB!")
            finally:
                cur.close()
                conn.close()

print(f"🎉 Pipeline 02 Complete!")
