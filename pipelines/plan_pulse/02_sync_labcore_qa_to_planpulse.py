# Databricks notebook source
# MAGIC %md
# MAGIC # PlanPulse 02: LabCore Clinker QA (esteem_lab_dev) -> PlanPulse (raw_qa_clinker)
# MAGIC Runs daily after Drive sync at 08:45 AM Asia/Colombo.

# COMMAND ----------
# MAGIC %pip install psycopg2-binary --quiet

# COMMAND ----------
import json
import os
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values


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


PARAM_TO_COL = {
    "CAO": "cao",
    "SIO2": "sio2",
    "AL2O3": "al2o3",
    "FE2O3": "fe2o3",
    "MGO": "mgo",
    "SO3": "so3",
    "CL": "cl",
    "LOI": "loi",
    "IR": "ir",
    "C3S": "c3s",
    "C2S": "c2s",
    "C3A": "c3a",
    "C4AF": "c4af",
    "MOISTURE": "moisture",
    "GRIND_150": "residue_150",
    "GRIND_300": "residue_300",
}


def main():
    pg_host, pg_port, pg_user = get_env_config()
    pg_password = get_secret("planpulse", "pg_password", "PG_PASSWORD") or "Vibhu2003"
    planpulse_db = "plan_pulse"
    labcore_db = "esteem_lab_dev"

    print(f"Syncing LabCore Clinker QA ({pg_host}:{pg_port}/{labcore_db}) -> {planpulse_db}.raw_qa_clinker")

    lab_conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=labcore_db)
    lab_cur = lab_conn.cursor()

    lab_cur.execute("""
        SELECT s.id, s.sample_code, s.shipment_no, s.clinker_source, s.arrival_date, s.remarks,
               pr.code AS param_code, r.value_numeric
        FROM public.samples s
        JOIN public.materials m ON m.id = s.material_id
        LEFT JOIN public.results r ON r.sample_id = s.id
        LEFT JOIN public.parameters pr ON pr.id = r.parameter_id
        WHERE m.code = 'CLINKER'
          AND s.sample_code IS NOT NULL
        ORDER BY s.arrival_date NULLS LAST, s.sample_code
    """)

    by_sample = {}
    for _sid, sample_code, ship_no, supplier, arrival_date, remarks, param_code, val_num in lab_cur.fetchall():
        rec = by_sample.setdefault(sample_code, {
            "sample_id": sample_code,
            "ship_no": ship_no or "",
            "supplier": supplier or "",
            "received_date": arrival_date.isoformat() if arrival_date else None,
            "remarks": remarks or "",
        })
        col = PARAM_TO_COL.get(param_code)
        if col and val_num is not None:
            rec[col] = float(val_num)

    lab_cur.close()
    lab_conn.close()

    rows = [
        (
            r["sample_id"], r["ship_no"], r["supplier"], r["received_date"],
            r.get("cao"), r.get("sio2"), r.get("al2o3"), r.get("fe2o3"), r.get("mgo"), r.get("so3"),
            r.get("cl"), r.get("loi"), r.get("ir"), r.get("c3s"), r.get("c2s"), r.get("c3a"), r.get("c4af"),
            r.get("moisture"), r.get("residue_150"), r.get("residue_300"), r["remarks"],
        )
        for r in by_sample.values()
    ]

    pp_conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=planpulse_db)
    pp_cur = pp_conn.cursor()

    sql = """
        INSERT INTO raw_qa_clinker (
            sample_id, ship_no, supplier, received_date,
            cao, sio2, al2o3, fe2o3, mgo, so3, cl, loi, ir,
            c3s, c2s, c3a, c4af, moisture, residue_150, residue_300, remarks
        ) VALUES %s
        ON CONFLICT (sample_id) DO UPDATE SET
            ship_no = COALESCE(NULLIF(raw_qa_clinker.ship_no, ''), EXCLUDED.ship_no),
            supplier = COALESCE(NULLIF(raw_qa_clinker.supplier, ''), EXCLUDED.supplier),
            received_date = COALESCE(raw_qa_clinker.received_date, EXCLUDED.received_date),
            cao = COALESCE(raw_qa_clinker.cao, EXCLUDED.cao),
            sio2 = COALESCE(raw_qa_clinker.sio2, EXCLUDED.sio2),
            al2o3 = COALESCE(raw_qa_clinker.al2o3, EXCLUDED.al2o3),
            fe2o3 = COALESCE(raw_qa_clinker.fe2o3, EXCLUDED.fe2o3),
            mgo = COALESCE(raw_qa_clinker.mgo, EXCLUDED.mgo),
            so3 = COALESCE(raw_qa_clinker.so3, EXCLUDED.so3),
            cl = COALESCE(raw_qa_clinker.cl, EXCLUDED.cl),
            loi = COALESCE(raw_qa_clinker.loi, EXCLUDED.loi),
            ir = COALESCE(raw_qa_clinker.ir, EXCLUDED.ir),
            c3s = COALESCE(raw_qa_clinker.c3s, EXCLUDED.c3s),
            c2s = COALESCE(raw_qa_clinker.c2s, EXCLUDED.c2s),
            c3a = COALESCE(raw_qa_clinker.c3a, EXCLUDED.c3a),
            c4af = COALESCE(raw_qa_clinker.c4af, EXCLUDED.c4af),
            moisture = COALESCE(raw_qa_clinker.moisture, EXCLUDED.moisture),
            residue_150 = COALESCE(raw_qa_clinker.residue_150, EXCLUDED.residue_150),
            residue_300 = COALESCE(raw_qa_clinker.residue_300, EXCLUDED.residue_300),
            remarks = COALESCE(NULLIF(raw_qa_clinker.remarks, ''), EXCLUDED.remarks)
    """
    if rows:
        execute_values(pp_cur, sql, rows, page_size=500)

    pp_cur.execute(
        """INSERT INTO sys_import_log (import_id, source_id, file_name, rows_read, rows_inserted, status, message)
           VALUES (%s, 'DATABRICKS_LABCORE', 'public.samples (CLINKER)', %s, %s, 'OK', 'Daily LabCore QA sync completed')""",
        (f"DBX-LAB-{int(datetime.now(timezone.utc).timestamp())}", len(rows), len(rows)),
    )
    pp_conn.commit()
    pp_cur.close()
    pp_conn.close()
    print(f"Done. Upserted {len(rows)} LabCore Clinker QA samples into {planpulse_db}.raw_qa_clinker.")


if __name__ == "__main__":
    main()
