#!/usr/bin/env python3
import mysql.connector
import os
import logging

# -------------------------------
# MySQL Connection (same as main)
# -------------------------------
def get_mysql_connection(database=None):
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "aws-rds-mysql-1.crtevtvwnjg4.us-west-1.rds.amazonaws.com"),
        user=os.getenv("MYSQL_USER", "admin"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=database,
        allow_local_infile=True
    )

# -------------------------------
# Fetch total DB size
# -------------------------------
def get_mysql_db_size(db_name="synthetic_db"):
    conn = get_mysql_connection()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT 
            SUM(data_length + index_length) AS total_bytes
        FROM information_schema.tables
        WHERE table_schema = '{db_name}'
    """)

    total_bytes = cur.fetchone()[0] or 0

    cur.close()
    conn.close()

    size_mb = total_bytes / (1024 ** 2)
    size_gb = total_bytes / (1024 ** 3)

    return total_bytes, size_mb, size_gb

# -------------------------------
# Per-table sizes
# -------------------------------
def get_mysql_table_sizes(db_name="synthetic_db"):
    conn = get_mysql_connection()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT 
            table_name,
            data_length + index_length AS size_bytes,
            table_rows
        FROM information_schema.tables
        WHERE table_schema = '{db_name}'
        ORDER BY size_bytes DESC;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# -------------------------------
# CLI / Standalone execution
# -------------------------------
def print_db_size(db_name="synthetic_db"):
    total_bytes, size_mb, size_gb = get_mysql_db_size(db_name)

    print("\n===============================")
    print(f" MySQL Database Size: {db_name}")
    print("===============================")
    print(f"Bytes : {total_bytes:,}")
    print(f"MB    : {size_mb:.2f} MB")
    print(f"GB    : {size_gb:.3f} GB\n")

    # Per table
    print("Per-table sizes:")
    table_sizes = get_mysql_table_sizes(db_name)
    for tbl, bytes_, rows in table_sizes:
        print(f"  {tbl:40s}  {bytes_/1024/1024:10.2f} MB   {rows:,} rows")

    print("\nDone.\n")

# -------------------------------
# Main
# -------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="synthetic_db", help="Database name to inspect")
    args = parser.parse_args()
    print_db_size(args.db)
