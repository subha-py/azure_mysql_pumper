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
MULTIPART_SIZE_MB = 100
DEFAULT_ROW_SIZE_BYTES = 60
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
        "parallelism": min(10, multiprocessing.cpu_count()),
        "csv_workers": multiprocessing.cpu_count(),
        "database": "synthetic_small_db",
    },
    "baseline": {
        "description": "Baseline ingestion performance",
        "target_size_gb": 100,
        "tables": 100,
        "parallelism": multiprocessing.cpu_count(),
        "csv_workers": multiprocessing.cpu_count(),
        "database": "synthetic_100gb_db",
    },
    "medium": {
        "description": "Medium-scale ingestion with high table count",
        "target_size_gb": 500,
        "tables": 1500,
        "parallelism": multiprocessing.cpu_count(),
        "csv_workers": multiprocessing.cpu_count(),
        "database": "synthetic_500gb_db",
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
    logging.info(f"🔍 Checking if database '{db_name}' exists...")
    conn = get_mysql_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cur.close()
    conn.close()
    logging.info(f"✓ Database '{db_name}' is ready")


# -------------------------------------------------------------------
# CSV GENERATION
# -------------------------------------------------------------------
def generate_csv_wrapper(args):
    """Wrapper for parallel CSV generation - must be at module level for pickling"""
    table_name, rows, scenario = args
    return generate_csv(table_name, rows, scenario)


def generate_csv(table_name, rows, scenario):
    start_time = time.time()
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
    
    # Don't log from worker processes - return info instead
    # logging.info(f"📝 Generating CSV for {table_name} with {rows:,} rows...")

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

    elapsed = time.time() - start_time
    # Return timing info instead of logging from worker process
    return [csv_path], total_bytes, rows, elapsed

# -------------------------------------------------------------------
# LOAD TO MYSQL
# -------------------------------------------------------------------
def load_csv_to_mysql(table_name, csv_files, db_name="synthetic_db"):
    start_time = time.time()
    logging.info(f"📤 Loading {table_name} to MySQL (database: {db_name})...")
    
    try:
        conn = get_mysql_connection(db_name)
        cur = conn.cursor()

        # cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        logging.info(f"  Creating table {table_name} if not exists...")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT,
                name VARCHAR(255),
                value DOUBLE,
                timestamp DATETIME
            )
        """)

        for csv_path in csv_files:
            logging.info(f"  Executing LOAD DATA LOCAL INFILE for {csv_path}...")
            load_sql = f"""
                LOAD DATA LOCAL INFILE '{csv_path}'
                INTO TABLE {table_name}
                FIELDS TERMINATED BY ',' 
                LINES TERMINATED BY '\n'
                IGNORE 1 LINES
                (id, name, value, timestamp)
            """
            cur.execute(load_sql)
            logging.info(f"  Committing transaction...")
            conn.commit()
            os.remove(csv_path)
            elapsed = time.time() - start_time
            logging.info(f"✓ Loaded {table_name} from {csv_path} in {elapsed:.2f}s and removed CSV.")

        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"❌ Error loading {table_name}: {e}")
        raise


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
def run_scenario(scenario_name, load_only=False):
    cfg = SCENARIOS.get(scenario_name, SCENARIOS[DEFAULT_SCENARIO])

    target_gb = cfg["target_size_gb"]
    tables = cfg["tables"]
    parallelism = cfg["parallelism"]
    csv_workers = cfg.get("csv_workers", multiprocessing.cpu_count())
    db_name = cfg.get("database", "synthetic_db")

    log_path = setup_logging(scenario_name)
    ensure_database_exists(db_name)

    # For small_test, clean up existing tables first
    if scenario_name == "small_test":
        conn = get_mysql_connection(db_name)
        cur = conn.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        cur.execute("SHOW TABLES")
        tables_to_drop = [row[0] for row in cur.fetchall()]
        if tables_to_drop:
            logging.info(f"🧹 Cleaning up {len(tables_to_drop)} existing tables in {db_name}...")
            for table in tables_to_drop:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
            logging.info(f"✓ Dropped {len(tables_to_drop)} tables")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        cur.close()
        conn.close()

    avg_row_bytes = DEFAULT_ROW_SIZE_BYTES

    total_target_bytes = target_gb * 1024 ** 3
    rows_per_table = int(total_target_bytes / (avg_row_bytes * tables))
    row_range = (rows_per_table, int(rows_per_table * 1.1))

    logging.info(f"Starting scenario '{scenario_name}' ({cfg['description']})")
    logging.info(f"Database: {db_name} | Target {target_gb}GB | {tables} tables | parallelism {parallelism} | CSV workers {csv_workers}")
    logging.info(f"Using avg_row_bytes ≈ {avg_row_bytes:.2f}, rows/table ≈ {rows_per_table:,}")

    def table_exists(table_name, check_db_name=None):
        if check_db_name is None:
            check_db_name = db_name
        conn = get_mysql_connection(check_db_name)
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (check_db_name, table_name))
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

    # Step 1: Generate CSV and load to MySQL in pipeline mode
    if load_only:
        logging.info(f"⏭️  Skipping CSV generation (--load-only mode)")
        # Discover existing CSV files
        instance_name = get_instance_name()
        csv_dir = Path(CSV_BASE_DIR) / instance_name / scenario_name
        
        if not csv_dir.exists():
            logging.error(f"❌ CSV directory not found: {csv_dir}")
            return None, 0
        
        csv_results = {}
        table_row_counts = {}
        
        for csv_file in csv_dir.glob("tbl_*.csv"):
            table_name = csv_file.stem
            csv_results[table_name] = [csv_file]
            # Count rows in CSV (subtract 1 for header)
            with open(csv_file) as f:
                row_count = sum(1 for _ in f) - 1
            table_row_counts[table_name] = row_count
            logging.info(f"✓ Found CSV for {table_name} with ~{row_count:,} rows")
        
        logging.info(f"✅ Found {len(csv_results)} CSV files ready to load")
        
        # Load existing CSVs
        logging.info(f"📥 Loading {len(csv_results)} tables to MySQL using {parallelism} workers...")
        
        def load_table(table_name):
            csv_files = csv_results[table_name]
            load_csv_to_mysql(table_name, csv_files, db_name=db_name)
            return table_row_counts[table_name]
        
        total_rows = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as load_ex:
            load_futures = [load_ex.submit(load_table, tname) for tname in csv_results.keys()]
            for f in concurrent.futures.as_completed(load_futures):
                total_rows += f.result()
    else:
        # Pipeline mode: Generate CSV -> Load -> Delete immediately
        logging.info(f"🚀 Generating and loading {tables} tables in pipeline mode using {parallelism} workers...")
        
        def process_table_pipeline(i):
            """Generate CSV, load to MySQL, and delete CSV immediately"""
            table_name = f"tbl_{i}"
            pipeline_start = time.time()
            
            try:
                logging.info(f"🔄 Processing table {i+1}/{tables}: {table_name}")
                
                # Skip if table already exists
                if table_exists(table_name):
                    logging.info(f"⏭️  Table {table_name} already exists — skipping.")
                    return 0
                
                # Generate CSV
                rows = random.randint(*row_range)
                logging.info(f"📝 Generating CSV for {table_name} with {rows:,} rows...")
                csv_files, _, actual_rows, gen_time = generate_csv(table_name, rows, scenario_name)
                logging.info(f"✓ Generated {table_name}.csv ({actual_rows:,} rows) in {gen_time:.2f}s")
                
                # Load to MySQL
                load_csv_to_mysql(table_name, csv_files, db_name=db_name)
                
                elapsed = time.time() - pipeline_start
                logging.info(f"✅ Completed {table_name} with {rows:,} rows in {elapsed:.2f}s (total pipeline)")
                return rows
            except Exception as e:
                logging.error(f"❌ Error processing {table_name}: {e}")
                raise
        
        total_rows = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as ex:
            futures = [ex.submit(process_table_pipeline, i) for i in range(tables)]
            for f in concurrent.futures.as_completed(futures):
                total_rows += f.result()

    logging.info(f"📊 Calculating final database size...")
    size_gb = get_mysql_db_size_gb(db_name=db_name)
    logging.info(f"✅ Final DB size {size_gb:.2f}GB with {total_rows:,} rows")

    if total_rows > 0:
        measured_row_bytes = (size_gb * 1024 ** 3) / total_rows
        logging.info(f"📏 Measured average row size: {measured_row_bytes:.2f} bytes")

    logging.info("🎯 Ingestion complete.")
    return size_gb, total_rows


# -------------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO, help="Scenario name (small_test, baseline, medium)")
    parser.add_argument("--load-only", action="store_true", help="Skip CSV generation and only load existing CSVs to MySQL")
    args = parser.parse_args()
    run_scenario(args.scenario, load_only=args.load_only)
