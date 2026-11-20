# Databricks notebook source
"""
Database Utility Module for SYNAP Data Engine
Handles PostgreSQL JDBC and psycopg2 connections for Dev and Prod
"""
import psycopg2
from pyspark.sql import SparkSession

def get_jdbc_url(host: str, port: int, dbname: str) -> str:
    return f"jdbc:postgresql://{host}:{port}/{dbname}"

def write_spark_df_to_postgres(df, table_name: str, host: str, port: int, dbname: str, user: str, password: str, mode: str = "append"):
    """
    Writes a PySpark DataFrame directly to PostgreSQL via JDBC
    """
    url = get_jdbc_url(host, port, dbname)
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()
    print(f"Successfully wrote {df.count()} rows to {table_name} ({mode})")

def execute_sql(sql: str, params=None, host: str = "35.200.154.39", port: int = 5435, dbname: str = "market_reach", user: str = "postgres", password: str = "Vibhu2003"):
    """
    Executes raw SQL query on PostgreSQL (for upserts, indexes, deletes)
    """
    conn = psycopg2.connect(host=host, port=port, database=dbname, user=user, password=password, connect_timeout=15)
    cur = conn.cursor()
    cur.execute(sql, params or ())
    conn.commit()
    cur.close()
    conn.close()
