#!/usr/bin/env python3
import os
import csv
import json
import math
import time
import random
import logging
import decimal
import shutil
import mysql.connector
import concurrent.futures
from pathlib import Path

# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------
CSV_BASE_DIR = "/space/azure_mysql_csvs"
LOG_DIR = "logs"
ROW_SIZE_CACHE = "row_size_estimate.json"
MULTIPART_SIZE_MB = 100

SCENARIOS = {
    "small_test": {
        "description": "Minimal load sanity test",
        "target_size_gb": 1,
        "tables": 5,
        "parallelism": 5,
    },
    "baseline": {
        "description": "Baseline ingestion performance",
        "target_size_gb": 100,
        "tables": 100,
        "parallelism": 10,
    },
    "medium": {
        "description": "Medium-scale ingestion with high table count",
        "target_size_gb": 500,
        "tables": 1500,
        "parallelism": 25,
    },
}

DEFAULT_SCENARIO = "small_test"

# -------------------------------------------------------------------
# LOGGING SETUP
# -------------------------------------------------------------------
def setup_logging(scenario):
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"{scenario}.log")
    if os.path.exists(log_path):
        os.remove(log_path)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s", "%H:%M:%S")

    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    logging.info(f"Logging → {log_path}")
    return log_path


# -------------------------------------------------------------------
# MYSQL CONNECTION
# -------------------------------------------------------------------
def get_mysql_connection(database=None):
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "sbera-500gb-500tables.mysql.database.azure.com"),
        user=os.getenv("MYSQL_USER", "adminuser"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=database,
        allow_local_infile=True
    )


def ensure_database_exists(db_name="synthetic_db"):
    conn = get_mysql_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cur.close()
    conn.close()
    logging.info(f"Ensured database exists: {db_name}")


# -------------------------------------------------------------------
# ROW SIZE ESTIMATE CACHE
# -------------------------------------------------------------------
def load_row_size_estimates():
    if os.path.exists(ROW_SIZE_CACHE):
        with open(ROW_SIZE_CACHE) as f:
            return json.load(f)
    return {}


def save_row_size_estimates(est):
    def convert(o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return o
    with open(ROW_SIZE_CACHE, "w") as f:
        json.dump(est, f, indent=2, default=convert)
    logging.info("💡 Updated row size estimate cache saved.")


# -------------------------------------------------------------------
# CSV GENERATION
# -------------------------------------------------------------------
def generate_csv(table_name, rows, scenario):
    out_dir = Path(CSV_BASE_DIR) / scenario
    out_dir.mkdir(parents=True, exist_ok=True)

    total_bytes = 0
    csv_files = []
    part = 0
    headers = ["id", "name", "value", "timestamp"]

    rows_per_chunk = max(1, int((MULTIPART_SIZE_MB * 1024 * 1024) / 100))  # estimate

    while rows > 0:
        part_rows = min(rows, rows_per_chunk)
        part_path = out_dir / f"{table_name}_part{part}.csv"
        csv_files.append(part_path)
        with open(part_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for i in range(part_rows):
                row = [
                    i,
                    f"name_{random.randint(1, 999999)}",
                    random.random() * 1000,
                    time.strftime("%Y-%m-%d %H:%M:%S")
                ]
                writer.writerow(row)
                total_bytes += sum(len(str(x)) for x in row) + 10
        logging.info(f"Generated {part_rows} rows → {part_path}")
        rows -= part_rows
        part += 1

    return csv_files, total_bytes


# -------------------------------------------------------------------
# LOAD TO MYSQL
# -------------------------------------------------------------------
def load_csv_to_mysql(table_name, csv_files, db_name="synthetic_db"):
    conn = get_mysql_connection(db_name)
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    cur.execute(f"""
        CREATE TABLE {table_name} (
            id INT,
            name VARCHAR(255),
            value DOUBLE,
            timestamp DATETIME
        )
    """)

    for csv_path in csv_files:
        load_sql = f"""
            LOAD DATA LOCAL INFILE '{csv_path}'
            INTO TABLE {table_name}
            FIELDS TERMINATED BY ',' 
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (id, name, value, timestamp)
        """
        cur.execute(load_sql)
        conn.commit()
        os.remove(csv_path)
        logging.info(f"Loaded {table_name} from {csv_path} and removed CSV.")

    cur.close()
    conn.close()


# -------------------------------------------------------------------
# DB SIZE
# -------------------------------------------------------------------
def get_mysql_db_size_gb(db_name="synthetic_db"):
    conn = get_mysql_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT SUM(data_length + index_length)
        FROM information_schema.tables
        WHERE table_schema = '{db_name}'
    """)
    size_bytes = cur.fetchone()[0] or 0
    cur.close()
    conn.close()
    return size_bytes / (1024 ** 3)


# -------------------------------------------------------------------
# MAIN INGESTION LOGIC
# -------------------------------------------------------------------
def run_scenario(scenario_name):
    cfg = SCENARIOS.get(scenario_name, SCENARIOS[DEFAULT_SCENARIO])

    target_gb = cfg["target_size_gb"]
    tables = cfg["tables"]
    parallelism = cfg["parallelism"]

    log_path = setup_logging(scenario_name)
    ensure_database_exists("synthetic_db")

    estimates = load_row_size_estimates()
    avg_row_bytes = estimates.get(scenario_name, 100.0)

    total_target_bytes = target_gb * 1024 ** 3
    rows_per_table = int(total_target_bytes / (avg_row_bytes * tables))
    row_range = (rows_per_table, int(rows_per_table * 1.1))

    logging.info(f"Starting scenario '{scenario_name}' ({cfg['description']})")
    logging.info(f"Target {target_gb}GB | {tables} tables | parallelism {parallelism}")
    logging.info(f"Using avg_row_bytes ≈ {avg_row_bytes:.2f}, rows/table ≈ {rows_per_table:,}")

    def process_table(i):
        table_name = f"tbl_{i}"
        rows = random.randint(*row_range)
        csv_files, _ = generate_csv(table_name, rows, scenario_name)
        load_csv_to_mysql(table_name, csv_files)
        return rows

    total_rows = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as ex:
        futures = [ex.submit(process_table, i) for i in range(tables)]
        for f in concurrent.futures.as_completed(futures):
            total_rows += f.result()

    size_gb = get_mysql_db_size_gb()
    logging.info(f"✅ Final DB size {size_gb:.2f}GB with {total_rows:,} rows")

    measured_row_bytes = (size_gb * 1024 ** 3) / total_rows
    logging.info(f"📏 Measured average row size: {measured_row_bytes:.2f} bytes")

    estimates[scenario_name] = measured_row_bytes
    save_row_size_estimates(estimates)

    logging.info("🎯 Ingestion complete — cache updated.")
    return size_gb, total_rows


# -------------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO, help="Scenario name (small_test, baseline, medium)")
    args = parser.parse_args()
    run_scenario(args.scenario)
