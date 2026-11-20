# Databricks notebook source
"""
Change Detection & File Signature Tracker for SYNAP Data Engine
Tracks file modification timestamps and MD5 hashes in PostgreSQL 'files_seen' table.
Exits pipeline immediately if no changes are detected.
"""
import psycopg2

def has_file_changed(file_id: str, current_sig: str, host: str, port: int, dbname: str, user: str, password: str) -> bool:
    """
    Checks if a file in Google Drive has changed compared to last recorded signature in 'files_seen'.
    Returns True if changed or new, False if identical (no work needed).
    """
    conn = psycopg2.connect(host=host, port=port, database=dbname, user=user, password=password, connect_timeout=15)
    cur = conn.cursor()
    try:
        cur.execute("SELECT modified FROM files_seen WHERE id = %s;", (file_id,))
        row = cur.fetchone()
        if not row:
            return True
        last_sig = row[0]
        return last_sig != current_sig
    finally:
        cur.close()
        conn.close()

def mark_file_seen(file_id: str, kind: str, name: str, current_sig: str, note: str, host: str, port: int, dbname: str, user: str, password: str):
    """
    Records the latest signature of a processed file in 'files_seen' table.
    """
    conn = psycopg2.connect(host=host, port=port, database=dbname, user=user, password=password, connect_timeout=15)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO files_seen (id, kind, name, modified, note, at)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                modified = EXCLUDED.modified,
                note = EXCLUDED.note,
                at = now();
        """, (file_id, kind, name, current_sig, note))
        conn.commit()
    finally:
        cur.close()
        conn.close()
