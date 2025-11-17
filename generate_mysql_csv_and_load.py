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
from io import StringIO
import multiprocessing

# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------
CSV_BASE_DIR = "/space/azure_mysql_csvs"
LOG_DIR = "logs"
ROW_SIZE_CACHE = "row_size_estimate.json"
MULTIPART_SIZE_MB = 100
HOST = os.getenv("MYSQL_HOST", "sbera-aurora-mysql.cluster-crtevtvwnjg4.us-west-1.rds.amazonaws.com")

def get_instance_name():
    """Extract instance name from MYSQL_HOST environment variable."""
    host = os.getenv("MYSQL_HOST", HOST)
    # Extract instance name (first part before first dot)
    return host.split('.')[0]

SCENARIOS = {
    "small_test": {
        "description": "Minimal load sanity test",
        "target_size_gb": 1,
        "tables": 5,
        "parallelism": min(10, multiprocessing.cpu_count() // 2),
        "csv_workers": multiprocessing.cpu_count() // 2,
    },
    "baseline": {
        "description": "Baseline ingestion performance",
        "target_size_gb": 100,
        "tables": 100,
        "parallelism": multiprocessing.cpu_count() // 2,
        "csv_workers": multiprocessing.cpu_count() // 2,
    },
    "medium": {
        "description": "Medium-scale ingestion with high table count",
        "target_size_gb": 500,
        "tables": 1500,
        "parallelism": multiprocessing.cpu_count() // 2,
        "csv_workers": multiprocessing.cpu_count() // 2,
    },
}

DEFAULT_SCENARIO = "small_test"

# -------------------------------------------------------------------
# LOGGING SETUP
# -------------------------------------------------------------------
def setup_logging(scenario):
    instance_name = get_instance_name()
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"{instance_name}_{scenario}.log")
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
        host=os.getenv("MYSQL_HOST", HOST),
        user=os.getenv("MYSQL_USER", "admin"),
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
def generate_csv_wrapper(args):
    """Wrapper for parallel CSV generation - must be at module level for pickling"""
    table_name, rows, scenario = args
    return generate_csv(table_name, rows, scenario)


def generate_csv(table_name, rows, scenario):
    instance_name = get_instance_name()
    out_dir = Path(CSV_BASE_DIR) / instance_name / scenario
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / f"{table_name}.csv"
    headers = ["id", "name", "value", "timestamp"]

    # Pre-generate timestamp once
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Batch size for writing
    batch_size = 10000
    total_bytes = 0

    with open(csv_path, "w", newline="", buffering=8192*16) as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        # Generate in batches for better performance
        for batch_start in range(0, rows, batch_size):
            batch_end = min(batch_start + batch_size, rows)
            batch = []
            
            for i in range(batch_start, batch_end):
                row = [
                    i,
                    f"name_{random.randint(1, 999999)}",
                    round(random.random() * 1000, 2),
                    timestamp
                ]
                batch.append(row)
                total_bytes += 50  # Approximate row size
            
            writer.writerows(batch)

    # Return success without logging (ProcessPoolExecutor issue)
    return [csv_path], total_bytes

# -------------------------------------------------------------------
# LOAD TO MYSQL
# -------------------------------------------------------------------
def load_csv_to_mysql(table_name, csv_files, db_name="synthetic_db"):
    conn = get_mysql_connection(db_name)
    cur = conn.cursor()

    # cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
    csv_workers = cfg.get("csv_workers", multiprocessing.cpu_count())

    log_path = setup_logging(scenario_name)
    ensure_database_exists("synthetic_db")

    estimates = load_row_size_estimates()
    avg_row_bytes = estimates.get(scenario_name, 100.0)

    total_target_bytes = target_gb * 1024 ** 3
    rows_per_table = int(total_target_bytes / (avg_row_bytes * tables))
    row_range = (rows_per_table, int(rows_per_table * 1.1))

    logging.info(f"Starting scenario '{scenario_name}' ({cfg['description']})")
    logging.info(f"Target {target_gb}GB | {tables} tables | parallelism {parallelism} | CSV workers {csv_workers}")
    logging.info(f"Using avg_row_bytes ≈ {avg_row_bytes:.2f}, rows/table ≈ {rows_per_table:,}")

    def table_exists(table_name, db_name="synthetic_db"):
        conn = get_mysql_connection(db_name)
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (db_name, table_name))
        exists = cur.fetchone()[0] > 0
        cur.close()
        conn.close()
        return exists


    def process_table(i):
        table_name = f"tbl_{i}"

        # ✨ Skip processing if table exists
        if table_exists(table_name):
            logging.info(f"⏭️  Table {table_name} already exists — skipping row generation.")
            return 0   # No rows generated

        # Normal path if table does NOT exist
        rows = random.randint(*row_range)
        return (table_name, rows)

    # Step 1: Generate all CSV files in parallel using all CPU cores
    logging.info(f"🚀 Generating CSVs using {csv_workers} workers...")
    tables_to_process = []
    
    for i in range(tables):
        table_name = f"tbl_{i}"
        if not table_exists(table_name):
            rows = random.randint(*row_range)
            tables_to_process.append((table_name, rows, scenario_name))
    
    csv_results = {}
    table_row_counts = {}
    with concurrent.futures.ProcessPoolExecutor(max_workers=csv_workers) as csv_ex:
        csv_futures = {csv_ex.submit(generate_csv_wrapper, args): args for args in tables_to_process}
        for future in concurrent.futures.as_completed(csv_futures):
            table_name, rows, scenario = csv_futures[future]
            csv_files, total_bytes = future.result()
            csv_results[table_name] = csv_files
            table_row_counts[table_name] = rows
            logging.info(f"✓ Generated {rows:,} rows for {table_name}")

    logging.info(f"✅ CSV generation complete: {len(csv_results)} files ready")

    # Step 2: Load CSVs to MySQL in parallel
    logging.info(f"📥 Loading {len(csv_results)} tables to MySQL using {parallelism} workers...")
    
    def load_table(table_name):
        csv_files = csv_results[table_name]
        load_csv_to_mysql(table_name, csv_files)
        return table_row_counts[table_name]
    
    total_rows = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as load_ex:
        load_futures = [load_ex.submit(load_table, tname) for tname in csv_results.keys()]
        for f in concurrent.futures.as_completed(load_futures):
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
