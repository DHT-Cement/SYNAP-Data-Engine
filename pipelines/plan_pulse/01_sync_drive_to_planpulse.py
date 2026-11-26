# Databricks notebook source
# MAGIC %md
# MAGIC # PlanPulse 01: Google Drive (DHT-PROD_MDB_Master) -> PostgreSQL (plan_pulse)
# MAGIC Runs daily at 08:45 AM Asia/Colombo.

# COMMAND ----------
# MAGIC %pip install psycopg2-binary openpyxl google-auth requests --quiet

# COMMAND ----------
import io
import json
import os
from datetime import datetime, date, timezone
import requests
import openpyxl
import psycopg2
from psycopg2.extras import execute_values
from google.oauth2 import service_account
from google.auth.transport.requests import Request as GoogleAuthRequest

MASTER_SHEET_ID = "1owTPf5Z2zebjO9diyKPVz8jkZU3h_xD_oU4yCXxazRo"


def get_env_config():
    try:
        dbutils.widgets.text("env", "dev", "Environment")  # type: ignore  # noqa: F821
        target_env = dbutils.widgets.get("env").strip().lower()  # type: ignore  # noqa: F821
    except Exception:
        target_env = os.environ.get("PLANPULSE_ENV", "dev").strip().lower()

    cfg_path = (
        "/Workspace/Users/engproject@dhtcement.lk/SYNAP-Data-Engine/config/prod.json"
        if target_env in ("prod", "production")
        else "/Workspace/Users/engproject@dhtcement.lk/SYNAP-Data-Engine/config/dev.json"
    )
    if os.path.exists(cfg_path):
        with open(cfg_path, "r") as f:
            cfg = json.load(f)
        return cfg["database"]["host"], int(cfg["database"]["port"]), cfg["database"]["user"]
    return (
        os.environ.get("PG_HOST", "35.200.154.39"),
        int(os.environ.get("PG_PORT", "5435")),
        os.environ.get("PG_USER", "postgres"),
    )


def get_secret(scope, key, env_fallback=None):
    try:
        return dbutils.secrets.get(scope=scope, key=key)  # type: ignore  # noqa: F821
    except Exception:
        return os.environ.get(env_fallback or key.upper())


def get_drive_token():
    sa_json = get_secret("planpulse", "gdrive_sa_json", "GDRIVE_SA_JSON")
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]

    if sa_json:
        creds = service_account.Credentials.from_service_account_info(json.loads(sa_json), scopes=scopes)
    else:
        candidate_paths = [
            os.environ.get("GDRIVE_SA_PATH", ""),
            "/Workspace/Users/engproject@dhtcement.lk/secrets/service_account.json",
            r"C:\Users\Innovus\Downloads\synap-drive-reader-a2892879b1ed.json",
        ]
        sa_path = next((p for p in candidate_paths if p and os.path.exists(p)), None)
        if not sa_path:
            raise FileNotFoundError("Google Drive service account JSON not found in secrets or Workspace.")
        creds = service_account.Credentials.from_service_account_file(sa_path, scopes=scopes)

    creds.refresh(GoogleAuthRequest())
    return creds.token


def export_master_workbook(token):
    url = (
        f"https://www.googleapis.com/drive/v3/files/{MASTER_SHEET_ID}/export"
        "?mimeType=application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        "&supportsAllDrives=true"
    )
    res = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    res.raise_for_status()
    return openpyxl.load_workbook(io.BytesIO(res.content), data_only=True, read_only=True)


def to_num(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace(",", "").replace("%", "").strip()
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def to_int(v):
    n = to_num(v)
    return int(round(n)) if n is not None else None


def to_bool(v):
    if v is None or v == "":
        return None
    s = str(v).strip().upper()
    if s in ("Y", "YES", "TRUE", "1"):
        return True
    if s in ("N", "NO", "FALSE", "0"):
        return False
    return None


def to_date(v):
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    s = str(v).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return None


def to_ts(v):
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day).isoformat()
    s = str(v).strip()
    return s if s else None


def to_str(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    s = str(v).strip()
    return s if s else None


def sheet_rows(wb, name):
    if name not in wb.sheetnames:
        return []
    ws = wb[name]
    rows_iter = ws.iter_rows(values_only=True)
    headers = next(rows_iter, None)
    if not headers:
        return []
    headers = [str(h).strip() if h is not None else "" for h in headers]
    out = []
    for row in rows_iter:
        if not any(cell is not None and str(cell).strip() != "" for cell in row):
            continue
        out.append({headers[i]: row[i] if i < len(row) else None for i in range(len(headers)) if headers[i]})
    return out


def upsert(cur, table, cols, conflict_cols, rows):
    if not rows:
        return 0
    seen = set()
    deduped = []
    key_idx = [cols.index(k) for k in conflict_cols]
    for r in rows:
        key = tuple(r[i] for i in key_idx)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    update_cols = [c for c in cols if c not in conflict_cols]
    update_clause = (
        f"DO UPDATE SET {', '.join(f'{c} = EXCLUDED.{c}' for c in update_cols)}"
        if update_cols else "DO NOTHING"
    )
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_cols)}) {update_clause}"
    )
    execute_values(cur, sql, deduped, page_size=500)
    return len(deduped)


def main():
    pg_host, pg_port, pg_user = get_env_config()
    pg_password = get_secret("planpulse", "pg_password", "PG_PASSWORD") or "Vibhu2003"
    planpulse_db = "plan_pulse"

    print(f"Syncing Google Drive -> PostgreSQL {pg_host}:{pg_port}/{planpulse_db}")
    token = get_drive_token()
    wb = export_master_workbook(token)

    conn = psycopg2.connect(
        host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=planpulse_db
    )
    cur = conn.cursor()

    def sync_tab(tab, table, cols, conflict_cols, mapper):
        src = sheet_rows(wb, tab)
        mapped = [row for r in src if (row := mapper(r)) is not None]
        n = upsert(cur, table, cols, conflict_cols, mapped)
        print(f"  {tab:26s} -> {table:24s} ({n} rows)")
        return n

    total = 0
    total += sync_tab("CFG_Settings", "cfg_settings", ["key", "value", "description"], ["key"],
        lambda r: [to_str(r.get("Key")), to_str(r.get("Value")), to_str(r.get("Description"))] if to_str(r.get("Key")) else None)

    total += sync_tab("MST_Equipment", "mst_equipment",
        ["equipment_id", "equipment_name", "section", "parent_id", "equipment_type", "criticality", "d365_asset_id", "track_kpi", "active"], ["equipment_id"],
        lambda r: [to_str(r.get("EquipmentID")), to_str(r.get("EquipmentName")) or to_str(r.get("EquipmentID")), to_str(r.get("Section")), to_str(r.get("ParentID")), to_str(r.get("EquipmentType")), to_str(r.get("Criticality")), to_str(r.get("D365_AssetID")), to_bool(r.get("TrackKPI")), to_bool(r.get("Active"))] if to_str(r.get("EquipmentID")) else None)

    total += sync_tab("MST_StopBenchmark", "mst_stop_benchmarks",
        ["bench_id", "name", "family", "type", "match_category", "match_equip", "match_text", "priority", "events", "min_min", "median_min", "avg_min", "p75_min", "max_min", "benchmark_min", "alert_min", "confidence", "note", "basis", "active", "loaded_at"], ["bench_id"],
        lambda r: [to_str(r.get("BenchID")), to_str(r.get("Name")), to_str(r.get("Family")), to_str(r.get("Type")), to_str(r.get("MatchCategory")), to_str(r.get("MatchEquip")), to_str(r.get("MatchText")), to_int(r.get("Priority")), to_int(r.get("Events")), to_num(r.get("MinMin")), to_num(r.get("MedianMin")), to_num(r.get("AvgMin")), to_num(r.get("P75Min")), to_num(r.get("MaxMin")), to_num(r.get("BenchmarkMin")), to_num(r.get("AlertMin")), to_str(r.get("Confidence")), to_str(r.get("Note")), to_str(r.get("Basis")), to_bool(r.get("Active")), to_ts(r.get("LoadedAt"))] if to_str(r.get("BenchID")) else None)

    total += sync_tab("MST_Materials", "mst_materials",
        ["material_code", "material_name", "category", "uom", "tolerance_pct", "moisture_applicable", "active"], ["material_code"],
        lambda r: [to_str(r.get("MaterialCode")), to_str(r.get("MaterialName")) or to_str(r.get("MaterialCode")), to_str(r.get("Category")), to_str(r.get("UoM")), to_num(r.get("TolerancePct")), to_bool(r.get("MoistureApplicable")), to_bool(r.get("Active"))] if to_str(r.get("MaterialCode")) else None)

    total += sync_tab("MST_Suppliers", "mst_suppliers",
        ["supplier_code", "supplier_name", "d365_vendor_account", "active"], ["supplier_code"],
        lambda r: [to_str(r.get("SupplierCode")), to_str(r.get("SupplierName")) or to_str(r.get("SupplierCode")), to_str(r.get("D365_VendorAccount")), to_bool(r.get("Active"))] if to_str(r.get("SupplierCode")) else None)

    total += sync_tab("MST_Products", "mst_products",
        ["product_code", "product_name", "active"], ["product_code"],
        lambda r: [to_str(r.get("ProductCode")), to_str(r.get("ProductName")) or to_str(r.get("ProductCode")), to_bool(r.get("Active"))] if to_str(r.get("ProductCode")) else None)

    total += sync_tab("MST_Shifts", "mst_shifts",
        ["shift_code", "start_time", "end_time", "hours"], ["shift_code"],
        lambda r: [to_str(r.get("ShiftCode")), to_str(r.get("StartTime")), to_str(r.get("EndTime")), to_num(r.get("Hours"))] if to_str(r.get("ShiftCode")) else None)

    total += sync_tab("MST_DowntimeCodes", "mst_downtime_codes",
        ["downtime_code", "category", "description", "counts_as_breakdown"], ["downtime_code"],
        lambda r: [to_str(r.get("DowntimeCode")), to_str(r.get("Category")), to_str(r.get("Description")), to_bool(r.get("CountsAsBreakdown"))] if to_str(r.get("DowntimeCode")) else None)

    total += sync_tab("RAW_RM_WBTickets", "raw_rm_wb_tickets",
        ["ticket_no", "in_date", "in_time", "vehicle", "driver", "movement", "material", "material_name", "shipment", "receive_from", "supplier", "container_in", "first_weight", "second_weight", "net_weight", "out_date", "unloaded_yn", "unloaded_date", "unloaded_time", "first_seen", "last_updated", "import_id"], ["ticket_no"],
        lambda r: [to_str(r.get("TicketNo")), to_date(r.get("InDate")), to_str(r.get("InTime")), to_str(r.get("Vehicle")), to_str(r.get("Driver")), to_str(r.get("Movement")), to_str(r.get("Material")), to_str(r.get("MaterialName")), to_str(r.get("Shipment")), to_str(r.get("ReceiveFrom")), to_str(r.get("Supplier")), to_str(r.get("ContainerIn")), to_num(r.get("FirstWeight")), to_num(r.get("SecondWeight")), to_num(r.get("NetWeight")), to_date(r.get("OutDate")), to_bool(r.get("UnloadedYN")), to_date(r.get("UnloadedDate")), to_str(r.get("UnloadedTime")), to_ts(r.get("FirstSeen")), to_ts(r.get("LastUpdated")), to_str(r.get("ImportID"))] if to_str(r.get("TicketNo")) else None)

    total += sync_tab("RAW_RM_Monthly", "raw_rm_monthly",
        ["month", "material", "stage", "containers", "qty_mt", "source", "note", "loaded_at"], ["month", "material", "stage"],
        lambda r: [to_str(r.get("Month")), to_str(r.get("Material")), to_str(r.get("Stage")), to_int(r.get("Containers")), to_num(r.get("Qty_MT")), to_str(r.get("Source")), to_str(r.get("Note")), to_ts(r.get("LoadedAt"))] if (to_str(r.get("Month")) and to_str(r.get("Material")) and to_str(r.get("Stage"))) else None)

    for tab, basis in (("RAW_RM_DailyStock", "7-7"), ("RAW_RM_DailyStock_00", "0-0")):
        total += sync_tab(tab, "raw_rm_daily_stock",
            ["day_basis", "stock_date", "material", "rec_cont", "rec_mt", "unl_cont", "unl_mt", "close_cont", "close_mt", "hopper_mt", "silo_mt", "total_mt", "source", "loaded_at"], ["day_basis", "stock_date", "material"],
            lambda r, b=basis: [b, to_date(r.get("Date")), to_str(r.get("Material")), to_int(r.get("Rec_Cont")), to_num(r.get("Rec_MT")), to_int(r.get("Unl_Cont")), to_num(r.get("Unl_MT")), to_int(r.get("Close_Cont")), to_num(r.get("Close_MT")), to_num(r.get("Hopper_MT")), to_num(r.get("Silo_MT")), to_num(r.get("Total_MT")), to_str(r.get("Source")), to_ts(r.get("LoadedAt"))] if (to_date(r.get("Date")) and to_str(r.get("Material"))) else None)

    for tab, basis in (("RAW_PRD_DailyReport", "7-7"), ("RAW_PRD_DailyReport_00", "0-0")):
        total += sync_tab(tab, "raw_prd_daily_report",
            ["day_basis", "report_date", "product", "wet_mt", "dry_mt", "running_hrs", "prod_rate_tph", "cf_pct", "sec_mill", "sec_overall", "clinker_mt", "gypsum_mt", "calcite_mt", "flyash_mt", "kwh_mill_est", "kwh_overall_est", "remarks", "source", "loaded_at"], ["day_basis", "report_date", "product"],
            lambda r, b=basis: [b, to_date(r.get("Date")), to_str(r.get("Product")), to_num(r.get("Wet_MT")), to_num(r.get("Dry_MT")), to_num(r.get("RunningHrs")), to_num(r.get("ProdRate_TPH")), to_num(r.get("CF_Pct")), to_num(r.get("SEC_Mill")), to_num(r.get("SEC_Overall")), to_num(r.get("Clinker_MT")), to_num(r.get("Gypsum_MT")), to_num(r.get("Calcite_MT")), to_num(r.get("FlyAsh_MT")), to_num(r.get("kWh_Mill_Est")), to_num(r.get("kWh_Overall_Est")), to_str(r.get("Remarks")), to_str(r.get("Source")), to_ts(r.get("LoadedAt"))] if (to_date(r.get("Date")) and to_str(r.get("Product"))) else None)

    for tab, basis in (("RAW_PRD_DailyStoppage", "7-7"), ("RAW_PRD_DailyStoppage_00", "0-0")):
        total += sync_tab(tab, "raw_prd_daily_stoppage",
            ["day_basis", "report_date", "seq", "description", "hrs", "category", "category_rule", "category_override", "source", "loaded_at"], ["day_basis", "report_date", "seq"],
            lambda r, b=basis: [b, to_date(r.get("Date")), to_int(r.get("Seq")), to_str(r.get("Description")), to_num(r.get("Hrs")), to_str(r.get("Category")), to_str(r.get("CategoryRule")), to_str(r.get("CategoryOverride")), to_str(r.get("Source")), to_ts(r.get("LoadedAt"))] if (to_date(r.get("Date")) and to_int(r.get("Seq")) is not None) else None)

    total += sync_tab("RAW_PRD_MonthlyMill", "raw_prd_monthly_mill",
        ["month", "product", "material", "wet_mt", "dry_mt", "source", "loaded_at"], ["month", "product", "material"],
        lambda r: [to_str(r.get("Month")), to_str(r.get("Product")), to_str(r.get("Material")), to_num(r.get("Wet_MT")), to_num(r.get("Dry_MT")), to_str(r.get("Source")), to_ts(r.get("LoadedAt"))] if (to_str(r.get("Month")) and to_str(r.get("Product")) and to_str(r.get("Material"))) else None)

    total += sync_tab("RAW_PRD_MonthlyRun", "raw_prd_monthly_run",
        ["month", "product", "adjustment_wet_mt", "adjustment_dry_mt", "running_hrs", "power_incomer_kwh", "power_ht_motor_kwh", "power_ht_motor_vfd_kwh", "source", "loaded_at"], ["month", "product"],
        lambda r: [to_str(r.get("Month")), to_str(r.get("Product")), to_num(r.get("Adjustment_Wet_MT")), to_num(r.get("Adjustment_Dry_MT")), to_num(r.get("RunningHrs")), to_num(r.get("Power_Incomer_kWh")), to_num(r.get("Power_HTMotor_kWh")), to_num(r.get("Power_HTMotorVFD_kWh")), to_str(r.get("Source")), to_ts(r.get("LoadedAt"))] if (to_str(r.get("Month")) and to_str(r.get("Product"))) else None)

    total += sync_tab("RAW_PRD_MonthlyKPIInput", "raw_prd_monthly_kpi",
        ["month", "equipment", "planned_prod_hrs", "idle_hrs", "internal_breakdowns", "max_rate_tph", "quality_factor", "oee_target", "source", "loaded_at"], ["month", "equipment"],
        lambda r: [to_str(r.get("Month")), to_str(r.get("Equipment")), to_num(r.get("PlannedProdHrs")), to_num(r.get("IdleHrs")), to_int(r.get("InternalBreakdowns")), to_num(r.get("MaxRate_TPH")), to_num(r.get("QualityFactor")), to_num(r.get("OEE_Target")), to_str(r.get("Source")), to_ts(r.get("LoadedAt"))] if (to_str(r.get("Month")) and to_str(r.get("Equipment"))) else None)

    total += sync_tab("RAW_PRD_YearlyHistory", "raw_prd_yearly_history",
        ["year", "product", "production_wet_mt", "production_dry_mt", "running_hrs", "feed_rate_tph", "source", "loaded_at"], ["year", "product"],
        lambda r: [to_int(r.get("Year")), to_str(r.get("Product")), to_num(r.get("Production_Wet_MT")), to_num(r.get("Production_Dry_MT")), to_num(r.get("RunningHrs")), to_num(r.get("FeedRate_TPH")), to_str(r.get("Source")), to_ts(r.get("LoadedAt"))] if (to_int(r.get("Year")) is not None and to_str(r.get("Product"))) else None)

    total += sync_tab("RAW_GA_ClinkerFeed", "raw_ga_clinker_feed",
        ["feed_date", "shift", "shipment", "ship_no", "supplier", "received", "unloaded", "sheet_name"], ["feed_date", "shift", "shipment", "ship_no"],
        lambda r: [to_date(r.get("Date")), to_str(r.get("Shift")), to_str(r.get("Shipment")) or "", to_str(r.get("ShipNo")) or "", to_str(r.get("Supplier")), to_int(r.get("Received")), to_int(r.get("Unloaded")), to_str(r.get("Sheet"))] if (to_date(r.get("Date")) and to_str(r.get("Shift"))) else None)

    total += sync_tab("RAW_QA_Clinker", "raw_qa_clinker",
        ["sample_id", "ship_no", "supplier", "containers", "received_date", "cao", "sio2", "al2o3", "fe2o3", "mgo", "so3", "cl", "loi", "ir", "af", "c3s", "c2s", "c3a", "c4af", "str_2d", "str_7d", "str_28d", "moisture", "residue_150", "residue_300", "grind_feed_rate", "psd_gt50", "psd_30_50", "psd_3_30", "psd_lt3", "remarks"], ["sample_id"],
        lambda r: [to_str(r.get("SampleID")), to_str(r.get("ShipNo")), to_str(r.get("Supplier")), to_int(r.get("Containers")), to_date(r.get("ReceivedDate")), to_num(r.get("CaO")), to_num(r.get("SiO2")), to_num(r.get("Al2O3")), to_num(r.get("Fe2O3")), to_num(r.get("MgO")), to_num(r.get("SO3")), to_num(r.get("Cl")), to_num(r.get("LOI")), to_num(r.get("IR")), to_num(r.get("AF")), to_num(r.get("C3S")), to_num(r.get("C2S")), to_num(r.get("C3A")), to_num(r.get("C4AF")), to_num(r.get("Str2d")), to_num(r.get("Str7d")), to_num(r.get("Str28d")), to_num(r.get("Moisture")), to_num(r.get("Residue150")), to_num(r.get("Residue300")), to_num(r.get("GrindFeedRate")), to_num(r.get("PSD_gt50")), to_num(r.get("PSD_30_50")), to_num(r.get("PSD_3_30")), to_num(r.get("PSD_lt3")), to_str(r.get("Remarks"))] if to_str(r.get("SampleID")) else None)

    total += sync_tab("RAW_PRD_ProcessHourly", "raw_prd_process_hourly",
        ["log_date", "log_hour", "running", "mill_condition", "cement_type", "cal_pct", "gyp_pct", "clk_tph", "gyp_tph", "cal_tph", "flyash_tph", "feed_rate", "ga_gt", "mill_in_t", "cm_a", "cm_kw", "cm_sound", "spray2_m3h", "mill_out_t", "cement_t", "vent_bf_dp", "vent_fan_damper", "vent_fan_rpm", "vent_fan_a", "be_a", "cls_rpm", "cls_a", "cls_fan_damper", "cls_fan_rpm", "cls_fan_a", "fresh_air", "reject_tph", "fk174_a", "fk175_a", "fk_backp", "blaine", "res_45", "clk_hopper_m", "remark"], ["log_date", "log_hour"],
        lambda r: [to_date(r.get("Date")), to_str(r.get("Hour")), to_bool(r.get("Running")), to_str(r.get("MillCondition")), to_str(r.get("CementType")), to_num(r.get("Cal_pct")), to_num(r.get("Gyp_pct")), to_num(r.get("Clk_tph")), to_num(r.get("Gyp_tph")), to_num(r.get("Cal_tph")), to_num(r.get("FlyAsh_tph")), to_num(r.get("FeedRate")), to_num(r.get("GA_gt")), to_num(r.get("MillIn_T")), to_num(r.get("CM_A")), to_num(r.get("CM_kW")), to_num(r.get("CM_Sound")), to_num(r.get("Spray2_m3h")), to_num(r.get("MillOut_T")), to_num(r.get("Cement_T")), to_num(r.get("VentBF_DP")), to_num(r.get("VentFan_Damper")), to_num(r.get("VentFan_rpm")), to_num(r.get("VentFan_A")), to_num(r.get("BE_A")), to_num(r.get("Cls_rpm")), to_num(r.get("Cls_A")), to_num(r.get("ClsFan_Damper")), to_num(r.get("ClsFan_rpm")), to_num(r.get("ClsFan_A")), to_num(r.get("FreshAir")), to_num(r.get("Reject_tph")), to_num(r.get("FK174_A")), to_num(r.get("FK175_A")), to_num(r.get("FK_BackP")), to_num(r.get("Blaine")), to_num(r.get("Res45")), to_num(r.get("ClkHopper_m")), to_str(r.get("Remark"))] if (to_date(r.get("Date")) and to_str(r.get("Hour"))) else None)

    total += sync_tab("RAW_BD_Stoppage", "raw_bd_stoppage",
        ["stop_id", "stop_date", "month", "shift", "engineer", "cement_type", "start_dt", "end_dt", "duration_hrs", "equipment", "equip_code", "failure", "department", "dept_source", "description", "category", "source", "loaded_at"], ["stop_id"],
        lambda r: [to_str(r.get("StopID")), to_date(r.get("Date")), to_str(r.get("Month")), to_str(r.get("Shift")), to_str(r.get("Engineer")), to_str(r.get("CementType")), to_ts(r.get("StartDT")), to_ts(r.get("EndDT")), to_num(r.get("Duration_Hrs")), to_str(r.get("Equipment")), to_str(r.get("EquipCode")), to_str(r.get("Failure")), to_str(r.get("Department")), to_str(r.get("DeptSource")), to_str(r.get("Description")), to_str(r.get("Category")), to_str(r.get("Source")), to_ts(r.get("LoadedAt"))] if to_str(r.get("StopID")) else None)

    total += sync_tab("RAW_BD_Days", "raw_bd_days",
        ["log_date", "month", "logged_hrs", "shifts", "source", "loaded_at"], ["log_date"],
        lambda r: [to_date(r.get("Date")), to_str(r.get("Month")), to_num(r.get("LoggedHrs")), to_str(r.get("Shifts")), to_str(r.get("Source")), to_ts(r.get("LoadedAt"))] if to_date(r.get("Date")) else None)

    for tab, basis in (("RAW_DSP_Daily", "7-7"), ("RAW_DSP_Daily_00", "0-0")):
        total += sync_tab(tab, "raw_dsp_daily",
            ["day_basis", "dsp_date", "month", "open_silo_mt", "production_mt", "returns_mt", "hardened_mt", "bags_pt", "bags_sv", "bags_total", "bags_mt", "bulk_mt", "silo_dispatch_mt", "to_mobile_mt", "purchased_mt", "from_mobile_mt", "close_silo_mt", "mobile_dispatch_mt", "total_sale_mt", "source", "loaded_at"], ["day_basis", "dsp_date"],
            lambda r, b=basis: [b, to_date(r.get("Date")), to_str(r.get("Month")), to_num(r.get("OpenSilo_MT")), to_num(r.get("Production_MT")), to_num(r.get("Returns_MT")), to_num(r.get("Hardened_MT")), to_int(r.get("BagsPT")), to_int(r.get("BagsSV")), to_int(r.get("BagsTotal")), to_num(r.get("Bags_MT")), to_num(r.get("Bulk_MT")), to_num(r.get("SiloDispatch_MT")), to_num(r.get("ToMobile_MT")), to_num(r.get("Purchased_MT")), to_num(r.get("FromMobile_MT")), to_num(r.get("CloseSilo_MT")), to_num(r.get("MobileDispatch_MT")), to_num(r.get("TotalSale_MT")), to_str(r.get("Source")), to_ts(r.get("LoadedAt"))] if to_date(r.get("Date")) else None)

    cur.execute(
        """INSERT INTO sys_import_log (import_id, source_id, file_name, file_id, rows_read, rows_inserted, status, message)
           VALUES (%s, 'DATABRICKS_DRIVE', 'DHT-PROD_MDB_Master', %s, %s, %s, 'OK', 'Daily Drive sync completed')""",
        (f"DBX-DRIVE-{int(datetime.now(timezone.utc).timestamp())}", MASTER_SHEET_ID, total, total),
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Done. Total rows upserted: {total}")


if __name__ == "__main__":
    main()
