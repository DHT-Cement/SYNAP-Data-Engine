# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Dynamics 365 ERP Dispatches & Sales Lines Ingestion (With Real-Time Change Detection)
# MAGIC 
# MAGIC - Scans Google Drive `ERP Sales Drop` folder (`1Tz_raKMCVrrDjZwJo8eII7BREh_9SLsG`)
# MAGIC - **Change Detection:** Computes signature (`filename @ modifiedTime`). If no changes, exits in seconds.
# MAGIC - If new or updated dispatches detected, transforms and writes to PostgreSQL.

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
# MAGIC ### Step 2: Transformation Business Rules

# COMMAND ----------
ITEM_MAP = {'FP-0001': 'Powertech', 'FP-0002': 'Bulk', 'FP-0004': 'Saviya'}
PRODUCTS = ['Bulk', 'Powertech', 'Saviya']
BAG_KG = 50
VAT_PCT = 18.0
SSCL_PCT = 2.5
TAX_MULTIPLIER = (1.0 + VAT_PCT / 100.0) * (1.0 + SSCL_PCT / 100.0)
REVISED_DAYS = 2
BACKLOG_DAYS = 14

def round2(n):
    return round(float(n or 0), 2)

def transform_d365_shipments_and_lines(shipments_csv_text, lines_csv_text, flags_csv_text=None):
    ship_reader = list(csv.reader(io.StringIO(shipments_csv_text)))
    lines_reader = list(csv.reader(io.StringIO(lines_csv_text)))
    flags_reader = list(csv.reader(io.StringIO(flags_csv_text))) if flags_csv_text else []

    if len(ship_reader) < 2 or len(lines_reader) < 2:
        return {"days": {}, "lines": [], "excluded": []}

    sh = [c.strip() for c in ship_reader[0]]
    ln = [c.strip() for c in lines_reader[0]]
    fl = [c.strip() for c in flags_reader[0]] if flags_reader else []

    skip = {}
    if fl and len(flags_reader) > 1:
        f_no = fl.index('number') if 'number' in fl else -1
        f_rc = fl.index('reasonCode') if 'reasonCode' in fl else -1
        f_rv = fl.index('shipmentRevised') if 'shipmentRevised' in fl else -1
        if f_no >= 0:
            for r in flags_reader[1:]:
                no = r[f_no].strip() if len(r) > f_no else ""
                if not no: continue
                rc = r[f_rc].strip().upper() if f_rc >= 0 and len(r) > f_rc else ""
                rv = r[f_rv].strip().lower() if f_rv >= 0 and len(r) > f_rv else ""
                if rc == 'BACKLOG': skip[no] = 'BACKLOG'
                elif rv in ['yes', 'true', '1']: skip[no] = 'revised'

    s_no = sh.index('number')
    s_date = sh.index('postingDate')
    s_ord = sh.index('orderNumber') if 'orderNumber' in sh else -1
    s_cust = sh.index('customerNumber') if 'customerNumber' in sh else -1
    s_name = sh.index('customerName') if 'customerName' in sh else -1
    s_sp = sh.index('salesperson') if 'salesperson' in sh else -1
    s_terms = sh.index('paymentTermsCode') if 'paymentTermsCode' in sh else -1
    s_bill = sh.index('billToCustomerNumber') if 'billToCustomerNumber' in sh else s_cust
    s_bill_n = sh.index('billToName') if 'billToName' in sh else s_name
    s_doc = sh.index('invoiceDate') if 'invoiceDate' in sh else -1
    s_mod = sh.index('lastModifiedDateTime') if 'lastModifiedDateTime' in sh else -1

    posting = {}
    head = {}
    for r in ship_reader[1:]:
        if len(r) > s_no and r[s_no].strip():
            no = r[s_no].strip()
            p_date = r[s_date].strip()[:10] if len(r) > s_date else ""
            posting[no] = p_date
            head[no] = r

    for no, r in head.items():
        if no in skip: continue
        pd = posting.get(no, "")
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', pd): continue
        dd = r[s_doc].strip()[:10] if s_doc >= 0 and len(r) > s_doc else ""
        md = r[s_mod].strip()[:10] if s_mod >= 0 and len(r) > s_mod else ""
        try:
            pd_dt = datetime.strptime(pd, "%Y-%m-%d")
            if md and re.match(r'^\d{4}-\d{2}-\d{2}$', md):
                md_dt = datetime.strptime(md, "%Y-%m-%d")
                if (md_dt - pd_dt).days >= REVISED_DAYS:
                    skip[no] = f"changed in BC on {md} - credit note / revised"
            elif dd and re.match(r'^\d{4}-\d{2}-\d{2}$', dd):
                dd_dt = datetime.strptime(dd, "%Y-%m-%d")
                if (pd_dt - dd_dt).days > BACKLOG_DAYS:
                    skip[no] = f"backlog - order dated {dd}"
        except Exception:
            pass

    l_doc = ln.index('documentNo')
    l_item = ln.index('lineObjectNumber')
    l_uom = ln.index('unitOfMeasureCode')
    l_price = ln.index('unitPrice')
    l_qty = ln.index('quantity')
    l_disc = ln.index('discountPercent') if 'discountPercent' in ln else -1
    l_tax = ln.index('taxPercent') if 'taxPercent' in ln else -1

    days = {}
    detail = {}

    for r in lines_reader[1:]:
        if len(r) <= max(l_doc, l_item, l_qty, l_price): continue
        doc = r[l_doc].strip()
        iso = posting.get(doc, "")
        if not iso or not re.match(r'^\d{4}-\d{2}-\d{2}$', iso) or doc in skip:
            continue

        item = r[l_item].strip()
        prod = ITEM_MAP.get(item)
        if not prod: continue
        p_idx = PRODUCTS.index(prod)

        try:
            qty = float(r[l_qty]) if r[l_qty] else 0.0
            price = float(r[l_price]) if r[l_price] else 0.0
            disc = float(r[l_disc]) if l_disc >= 0 and r[l_disc] else 0.0
        except ValueError:
            continue

        uom = r[l_uom].strip().upper() if l_uom >= 0 else "BAG"
        mt = qty if uom == 'MT' else (qty * BAG_KG / 1000.0)
        rs = qty * price * (1.0 - disc / 100.0)

        if iso not in days:
            days[iso] = {"mt": {p: 0.0 for p in PRODUCTS}, "rs": {p: 0.0 for p in PRODUCTS}}
        days[iso]["mt"][prod] += mt
        days[iso]["rs"][prod] += rs

        h = head.get(doc, [])
        sell = h[s_cust].strip() if s_cust >= 0 and len(h) > s_cust else ""
        bill = h[s_bill].strip() if s_bill >= 0 and len(h) > s_bill else sell
        sell_name = h[s_name].strip() if s_name >= 0 and len(h) > s_name else ""
        bill_name = h[s_bill_n].strip() if s_bill_n >= 0 and len(h) > s_bill_n else sell_name
        sub_cust = sell_name if bill != sell else ""

        k = f"{doc}|{item}"
        if k not in detail:
            detail[k] = {
                "day": iso, "dn": doc, "order_no": h[s_ord].strip() if s_ord >= 0 and len(h) > s_ord else "",
                "cust_code": bill or sell, "cust_name": bill_name or sell_name, "sub_customer": sub_cust,
                "sales_person": h[s_sp].strip() if s_sp >= 0 and len(h) > s_sp else "", "district_erp": "",
                "terms": h[s_terms].strip() if s_terms >= 0 and len(h) > s_terms else "", "item": item,
                "product": p_idx, "uom": uom, "qty": 0.0, "mt": 0.0, "gross": 0.0, "net": 0.0, "price": price, "tax_pct": VAT_PCT
            }
        
        detail[k]["qty"] += qty
        detail[k]["mt"] += mt
        detail[k]["gross"] += rs
        detail[k]["net"] += rs / TAX_MULTIPLIER

    for iso in days:
        for p in PRODUCTS:
            days[iso]["mt"][p] = round2(days[iso]["mt"][p])
            days[iso]["rs"][p] = round2(days[iso]["rs"][p])

    out_lines = [v for v in detail.values() if abs(v["qty"]) > 1e-6]
    for v in out_lines:
        v["qty"] = round2(v["qty"])
        v["mt"] = round2(v["mt"])
        v["gross"] = round2(v["gross"])
        v["net"] = round2(v["net"])

    excluded = [{"dn": no, "day": posting.get(no, ""), "why": skip[no]} for no in skip if no in posting]
    return {"days": days, "lines": out_lines, "excluded": excluded}

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert to PostgreSQL

# COMMAND ----------
def upsert_to_postgres(transformed_data, source_filename="BC_Shipments.csv", sig=""):
    lines = transformed_data.get("lines", [])
    days = transformed_data.get("days", {})
    excluded = transformed_data.get("excluded", [])
    
    if not lines and not days:
        print("No lines to sync.")
        return 0

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=15)
    cur = conn.cursor()
    try:
        for day_iso, d_info in days.items():
            cur.execute("""
                INSERT INTO erp_days (day, file, mt, rs, excluded, at)
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (day) DO UPDATE SET
                    file = EXCLUDED.file, mt = EXCLUDED.mt, rs = EXCLUDED.rs, excluded = EXCLUDED.excluded, at = now();
            """, (day_iso, source_filename, json.dumps(d_info["mt"]), json.dumps(d_info["rs"]), json.dumps([x for x in excluded if x["day"] == day_iso])))

        affected_days = list(days.keys())
        if affected_days:
            cur.execute("DELETE FROM sales_lines WHERE source = 'erp' AND day = ANY(%s::date[]);", (affected_days,))

        batch_size = 500
        for i in range(0, len(lines), batch_size):
            chunk = lines[i:i + batch_size]
            vals = []
            for x in chunk:
                vals.append((
                    'erp', x["day"], x["dn"], x["order_no"], x["cust_code"], x["cust_name"],
                    x["sub_customer"], x["sales_person"], x["district_erp"], x["terms"],
                    x["item"], x["product"], x["uom"], x["qty"], x["mt"], x["price"],
                    x["gross"], x["tax_pct"], x["net"], source_filename
                ))
            cur.executemany("""
                INSERT INTO sales_lines (
                    source, day, dn, order_no, cust_code, cust_name, sub_customer, sales_person,
                    district_erp, terms, item, product, uom, qty, mt, price, gross, tax_pct, net, file
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, vals)

        if sig:
            # ym from source_filename or first day
            ym = affected_days[0][:7] if affected_days else datetime.now().strftime("%Y-%m")
            mark_file_seen(f"erp:{ym}", "erp", source_filename, sig, f"{len(lines)} lines synced ({affected_days[0]}..{affected_days[-1]})", DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)

        conn.commit()
        print(f"✅ SYNC COMPLETE: {len(affected_days)} days, {len(lines)} lines updated in {target_env.upper()} DB!")
        return len(lines)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Google Drive Scan & Change-Detection Execution Loop

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
print(f"Found {len(drive_files)} files in Google Drive folder '{ERP_FOLDER_ID}'.")

by_name = {f['name']: f for f in drive_files}
ship_files = [f for f in drive_files if re.match(r"^BC_Shipments_\d{4}-\d{2}-\d{2}", f['name'])]
ship_files.sort(key=lambda x: x['name'], reverse=True)

per_month = {}
for s in ship_files:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s['name'])
    if not m: continue
    as_of = m.group(1)
    ym = as_of[:7]
    if ym in per_month: continue
    lines_name = s['name'].replace('BC_Shipments_', 'BC_Lines_')
    flags_name = s['name'].replace('BC_Shipments_', 'BC_Flags_')
    l = by_name.get(lines_name)
    if not l: continue
    fl = by_name.get(flags_name)
    per_month[ym] = {'as_of': as_of, 'ship': s, 'lines': l, 'flags': fl}

total_synced_months = 0
for ym, m_info in sorted(per_month.items(), reverse=True):
    ship_f = m_info['ship']
    lines_f = m_info['lines']
    flags_f = m_info['flags']
    
    ship_mod = int(datetime.fromisoformat(ship_f['modifiedTime'].replace('Z', '+00:00')).timestamp() * 1000)
    lines_mod = int(datetime.fromisoformat(lines_f['modifiedTime'].replace('Z', '+00:00')).timestamp() * 1000)
    sig = f"{ship_f['name']}@{ship_mod}+{lines_f['name']}@{lines_mod}"
    if flags_f:
        flags_mod = int(datetime.fromisoformat(flags_f['modifiedTime'].replace('Z', '+00:00')).timestamp() * 1000)
        sig += f"+{flags_f['name']}@{flags_mod}"
        
    file_id = f"erp:{ym}"
    changed = has_file_changed(file_id, sig, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
    
    if not changed:
        print(f"⏩ [UNCHANGED] Month {ym}: {sig}")
        continue

    print(f"🚀 [PROCESSING] Month {ym} from {ship_f['name']} & {lines_f['name']}...")
    ship_text = drive_service.files().get_media(fileId=ship_f['id']).execute().decode('utf-8-sig', errors='replace')
    lines_text = drive_service.files().get_media(fileId=lines_f['id']).execute().decode('utf-8-sig', errors='replace')
    flags_text = drive_service.files().get_media(fileId=flags_f['id']).execute().decode('utf-8-sig', errors='replace') if flags_f else None
    
    transformed = transform_d365_shipments_and_lines(ship_text, lines_text, flags_text)
    synced = upsert_to_postgres(transformed, source_filename=ship_f['name'], sig=sig)
    total_synced_months += 1

print(f"🎉 Pipeline 01 Complete! {total_synced_months} month(s) updated.")
