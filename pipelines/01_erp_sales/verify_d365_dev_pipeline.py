# Databricks notebook source
# MAGIC %md
# MAGIC # Verification Test: D365 Pipelines against DEV Database
# MAGIC 
# MAGIC This test executes the transformation functions with simulated D365 dispatches & aging records and writes strictly to port 5435.

# COMMAND ----------
# MAGIC %run ./01_sync_erp_dispatches

# COMMAND ----------
# 1. Test D365 Dispatches Ingestion into DEV sales_lines and erp_days
sample_shipments = """number,postingDate,orderNumber,customerNumber,customerName,salesperson,paymentTermsCode,billToCustomerNumber,billToName,invoiceDate,lastModifiedDateTime
DN-2026-9001,2026-11-20,SO-1001,CUST-001,Nimal Hardware,SP-KAMAL,30D,CUST-001,Nimal Hardware,2026-11-20,2026-11-20 08:30:00
DN-2026-9002,2026-11-20,SO-1002,CUST-002,Gamini Enterprises,SP-SUNIL,CASH,CUST-002,Gamini Enterprises,2026-11-20,2026-11-20 09:15:00
"""

sample_lines = """documentNo,lineObjectNumber,unitOfMeasureCode,quantity,unitPrice,discountPercent,taxPercent
DN-2026-9001,FP-0001,BAG,400,2450.0,0.0,18.0
DN-2026-9002,FP-0002,MT,30,45000.0,2.0,18.0
"""

transformed = transform_d365_shipments_and_lines(sample_shipments, sample_lines)
print("Transformed days:", transformed["days"])
print(f"Transformed {len(transformed['lines'])} lines.")

# Write to Dev
synced_lines = upsert_to_dev_postgres(transformed, source_filename="TEST_D365_SAMPLE.csv")

# Verify Dev PostgreSQL directly
import psycopg2
conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
cur = conn.cursor()
cur.execute("SELECT count(*) FROM sales_lines WHERE file = 'TEST_D365_SAMPLE.csv';")
cnt = cur.fetchone()[0]
cur.close()
conn.close()

assert cnt == synced_lines, f"Verification failed: expected {synced_lines}, got {cnt}"
print(f"🎉 100% VERIFIED: Successfully wrote and verified {cnt} D365 lines in DEV {DB_NAME} on port {DB_PORT}!")
dbutils.notebook.exit(json.dumps({"verified_lines": cnt, "status": "SUCCESS", "target_port": DB_PORT}))
