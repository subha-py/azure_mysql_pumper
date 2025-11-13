#!/usr/bin/env python3
"""
generate_mysql_csv_and_load.py

Generates synthetic CSV data for three scenarios and bulk-loads them into an
Azure Database for MySQL Flexible Server using LOAD DATA LOCAL INFILE.

Features:
- Three scenarios: baseline (100GB,100 tables), medium (500GB,1500 tables), test (1GB,5 tables)
- Default scenario: 'test'
- Dynamic scaling toward the target GB using an adaptive JSON cache (row_size_estimates.json)
- Random schema per table and variable table sizes around the computed value
- Local CSVs saved under /space/azure_mysql_csvs/<scenario>/
- Deletes local CSV files after successful upload
- Timestamped logging to logs/<scenario>.log
- Measures final dataset size and updates JSON cache with measured avg row size
- Automatically reruns (purging prior created tables) until final DB size is within ±5% of target

Requirements:
- Python 3.8+
- pip install faker mysql-connector-python tqdm

Usage:
    export MYSQL_PASSWORD='...'
    python generate_mysql_csv_and_load.py --scenario baseline

Configuration: edit DB_CONFIG or set environment variables.
"""

import argparse
import csv
import json
import logging
import math
import os
import random
import sys
import time
from pathlib import Path
from multiprocessing import Pool
from functools import partial

import mysql.connector
from faker import Faker
from tqdm import tqdm

# --------------------------- CONFIGURATION ---------------------------

BASE_DIR = Path(__file__).resolve().parent
CA_CERT_PATH = BASE_DIR / "MysqlflexGlobalRootCa.crt.pem"

DB_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "sbera-500gb-500tables.mysql.database.azure.com"),
    "user": os.environ.get("MYSQL_USER", "adminuser"),
    "password": os.environ.get("MYSQL_PASSWORD", "<your-password>"),
    "database": os.environ.get("MYSQL_DATABASE", "synthetic_db"),
    "port": int(os.environ.get("MYSQL_PORT", 3306)),
    "ssl_ca": str(CA_CERT_PATH),
    "ssl_disabled": False,
    "ssl_verify_cert": True,
    "ssl_verify_identity": True,
    # allow_local_infile must be set to True to use LOAD DATA LOCAL INFILE
    "allow_local_infile": True,
}

OUTPUT_ROOT = Path(os.environ.get("OUTPUT_ROOT", "/space/azure_mysql_csvs"))
LOG_ROOT = Path(os.environ.get("LOG_ROOT", "logs"))
CACHE_FILE = Path(os.environ.get("CACHE_FILE", "row_size_estimates.json"))

# CSV writing settings
CSV_BATCH_ROWS = int(os.environ.get("CSV_BATCH_ROWS", 50000))  # rows per CSV part file
CSV_LINE_TERMINATOR = "\n"

# Parallelism
GEN_WORKERS = int(os.environ.get("GEN_WORKERS", 4))
LOAD_WORKERS = int(os.environ.get("LOAD_WORKERS", 4))

# Scenario definitions (target size bytes, table count)
SCENARIOS = {
    "baseline": {"target_gb": 100, "num_tables": 100},
    "medium": {"target_gb": 500, "num_tables": 1500},
    "test": {"target_gb": 1, "num_tables": 5},
}

# Random schema generation parameters
COLUMN_TYPE_POOL = [
    ("VARCHAR", lambda: random.choice([50, 100, 150, 255])),
    ("TEXT", lambda: None),
    ("INT", lambda: None),
    ("DECIMAL", lambda: (10, 2)),
    ("DATETIME", lambda: None),
]

# Variation around computed per-table rows (±percentage)
SIZE_VARIATION_PCT = 0.20

# Safety: max rows per table to avoid accidental runaway
MAX_ROWS_PER_TABLE = 50_000_000

# Rerun settings
MAX_RERUN_ATTEMPTS = int(os.environ.get("MAX_RERUN_ATTEMPTS", 5))
TARGET_TOLERANCE_PCT = float(os.environ.get("TARGET_TOLERANCE_PCT", 5.0))  # ±5% default

# --------------------------- UTILITIES ---------------------------

fake = Faker()
Faker.seed(0)


def ensure_dirs(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def setup_logging(scenario: str):
    ensure_dirs(LOG_ROOT)
    log_file = LOG_ROOT / f"{scenario}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Logging started for scenario: %s", scenario)


def load_cache():
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            logging.exception("Failed to read cache file, starting fresh")
    return {}


def save_cache(cache: dict):
    tmp = CACHE_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(cache, f, indent=2)
    tmp.replace(CACHE_FILE)


def human_readable_bytes(b: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if b < 1024.0:
            return f"{b:3.2f}{unit}"
        b /= 1024.0
    return f"{b:.2f}PB"


# --------------------------- SCHEMA & DATA GENERATION ---------------------------


def random_schema(table_name: str):
    """Return a schema list: [(col_name, type_desc), ...] and the SQL create statement."""
    ncols = random.randint(4, 12)
    cols = []
    for i in range(ncols):
        base, arg_fn = random.choice(COLUMN_TYPE_POOL)
        if base == "VARCHAR":
            length = arg_fn()
            cols.append((f"c{i+1}", f"VARCHAR({length})"))
        elif base == "TEXT":
            cols.append((f"c{i+1}", "TEXT"))
        elif base == "INT":
            cols.append((f"c{i+1}", "INT"))
        elif base == "DECIMAL":
            p, s = arg_fn()
            cols.append((f"c{i+1}", f"DECIMAL({p},{s})"))
        elif base == "DATETIME":
            cols.append((f"c{i+1}", "DATETIME"))
    # ensure first column is an id
    cols.insert(0, ("id", "BIGINT AUTO_INCREMENT PRIMARY KEY"))
    # build create table SQL (InnoDB)
    col_defs = ",\n    ".join([f"{name} {typ}" for name, typ in cols])
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {col_defs}\n) ENGINE=InnoDB;"
    return cols, create_sql


def row_generator(schema):
    """Return a function that generates one CSV row list for the given schema (excludes id)."""

    def gen():
        row = []
        for name, typ in schema[1:]:
            if typ.startswith("VARCHAR"):
                length = int(typ[typ.find("(") + 1 : typ.find(")")])
                s = fake.word()
                if length > 20 and random.random() < 0.6:
                    s = fake.sentence(nb_words=min(10, max(1, length // 10)))
                s = s[:length]
                row.append(s)
            elif typ == "TEXT":
                row.append(fake.text(max_nb_chars=random.randint(50, 1024)))
            elif typ == "INT":
                row.append(str(random.randint(0, 1_000_000)))
            elif typ.startswith("DECIMAL"):
                row.append(f"{random.random() * 10000:.2f}")
            elif typ == "DATETIME":
                row.append(fake.date_time_between(start_date='-5y', end_date='now').strftime("%Y-%m-%d %H:%M:%S"))
            else:
                row.append("")
        return row

    return gen


# --------------------------- CSV PART CREATION ---------------------------


def write_csv_part(path: Path, schema, rows, include_header=False):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        if include_header:
            writer.writerow([name for name, _ in schema[1:]])
        for r in rows:
            writer.writerow(r)


def generate_csv_for_table(scenario_root: Path, table_name: str, schema, rows_target: int):
    ensure_dirs(scenario_root)
    gen = row_generator(schema)
    part_files = []
    written = 0
    part_idx = 0
    pbar = tqdm(total=rows_target, desc=f"Gen {table_name}", leave=False)
    try:
        while written < rows_target:
            this_batch = min(CSV_BATCH_ROWS, rows_target - written)
            rows = [gen() for _ in range(this_batch)]
            part_idx += 1
            part_name = f"{table_name}_part{part_idx}.csv"
            part_path = scenario_root / part_name
            write_csv_part(part_path, schema, rows, include_header=False)
            part_files.append(str(part_path))
            written += this_batch
            pbar.update(this_batch)
    finally:
        pbar.close()
    return part_files, schema


# --------------------------- MYSQL: CREATE TABLE & LOAD ---------------------------


def mysql_connect():
    conn = mysql.connector.connect(**DB_CONFIG)
    return conn


def ensure_database_exists():
    """Create the configured database if it does not exist.

    Connects to the server without selecting a database and issues
    CREATE DATABASE IF NOT EXISTS <database>.
    Raises on error so the caller knows load will fail.
    """
    dbname = DB_CONFIG.get("database")
    if not dbname:
        logging.warning("No database configured in DB_CONFIG; skipping creation")
        return
    # connect without database selected
    cfg = DB_CONFIG.copy()
    cfg.pop("database", None)
    conn = None
    cur = None
    try:
        logging.info("Ensuring database exists: %s", dbname)
        conn = mysql.connector.connect(**cfg)
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{dbname}`")
        conn.commit()
        logging.info("Database ensured: %s", dbname)
    except Exception:
        logging.exception("Failed to ensure database %s exists", dbname)
        raise
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def create_table_in_db(conn, table_name: str, create_sql: str):
    cur = conn.cursor()
    try:
        logging.debug("Creating table %s", table_name)
        cur.execute(create_sql)
        conn.commit()
    finally:
        cur.close()


def load_csv_into_table(conn, table_name: str, schema, csv_paths):
    columns = [name for name, _ in schema[1:]]
    columns_sql = ",".join(columns)
    cur = conn.cursor()
    try:
        for p in csv_paths:
            # escape backslashes in path for Windows compatibility
            path_escaped = p.replace("\\", "\\\\")
            sql = (
                f"LOAD DATA LOCAL INFILE '{path_escaped}' INTO TABLE `{table_name}` "
                f"FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '{CSV_LINE_TERMINATOR}' "
                f"({columns_sql});"
            )
            logging.info("Loading file %s into %s", p, table_name)
            cur.execute(sql)
            conn.commit()
        return True
    except Exception:
        logging.exception("Failed to load CSV parts into table %s", table_name)
        return False
    finally:
        cur.close()


# --------------------------- SIZING & ADAPTIVE CACHE ---------------------------


def estimate_row_size_for_schema(cache: dict, schema_key: str, schema) -> float:
    if schema_key in cache:
        return float(cache[schema_key])
    size = 0.0
    for name, typ in schema[1:]:
        if typ.startswith("VARCHAR"):
            length = int(typ[typ.find("(") + 1 : typ.find(")")])
            size += min(256, length)
        elif typ == "TEXT":
            size += 512
        elif typ == "INT":
            size += 4
        elif typ.startswith("DECIMAL"):
            size += 8
        elif typ == "DATETIME":
            size += 8
        else:
            size += 16
    size += 24
    return float(size)


def compute_rows_per_table(target_bytes: int, num_tables: int, schema_row_estimates: list):
    total_est_per_row = sum(schema_row_estimates)
    rows_alloc = []
    for est in schema_row_estimates:
        if total_est_per_row == 0:
            frac = 1.0 / len(schema_row_estimates)
        else:
            frac = est / total_est_per_row
        bytes_for_table = target_bytes * frac
        if est <= 0:
            rows = 0
        else:
            rows = int(bytes_for_table // est)
        var = int(rows * SIZE_VARIATION_PCT)
        rows = rows + random.randint(-var, var)
        rows = max(1, rows)
        rows = min(rows, MAX_ROWS_PER_TABLE)
        rows_alloc.append(rows)
    return rows_alloc


# --------------------------- FINAL SIZE MEASUREMENT ---------------------------


def measure_db_size_and_row_counts(conn, database: str):
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT table_name, table_rows, data_length + index_length AS bytes FROM information_schema.tables "
            "WHERE table_schema = %s",
            (database,),
        )
        results = cur.fetchall()
        summary = {r[0]: {"rows": int(r[1] or 0), "bytes": int(r[2] or 0)} for r in results}
        total_bytes = sum(v["bytes"] for v in summary.values())
        total_rows = sum(v["rows"] for v in summary.values())
        return summary, total_bytes, total_rows
    finally:
        cur.close()


# --------------------------- WORKFLOW ---------------------------


schema_cache_global = None


def _schema_to_create_sql(table_name, schema):
    col_defs = ",\n    ".join([f"{name} {typ}" for name, typ in schema])
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {col_defs}\n) ENGINE=InnoDB;"


def _generate_table_helper(scenario_root, rows_alloc_list, idx):
    i = idx + 1
    table_name = f"tbl_{i}"
    schema, create_sql = random_schema(table_name)
    target_rows = rows_alloc_list[idx]
    logging.info("Generating for %s with target rows %d", table_name, target_rows)
    part_files, schema_ret = generate_csv_for_table(scenario_root, table_name, schema, target_rows)
    return table_name, part_files, schema_ret, target_rows


def _load_table_helper(task):
    table_name, part_files, schema = task
    conn = mysql_connect()
    try:
        create_sql = _schema_to_create_sql(table_name, schema)
        create_table_in_db(conn, table_name, create_sql)
        ok = load_csv_into_table(conn, table_name, schema, part_files)
        if ok:
            for p in part_files:
                try:
                    os.remove(p)
                except Exception:
                    logging.exception("failed to delete %s", p)
        return table_name, ok
    finally:
        conn.close()


def purge_tables(conn, prefix='tbl_'):
    """Drop tables matching prefix to allow a clean rerun."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE %s", (prefix + '%',))
        rows = cur.fetchall()
        for (tbl,) in rows:
            try:
                logging.info("Dropping table %s", tbl)
                cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            except Exception:
                logging.exception("Failed to drop %s", tbl)
        conn.commit()
    finally:
        cur.close()


def run_once(scenario: str, cache: dict):
    cfg = SCENARIOS[scenario]
    target_bytes = int(cfg["target_gb"] * 1024 ** 3)
    num_tables = int(cfg["num_tables"])
    logging.info("Scenario %s: target %s across %d tables", scenario, human_readable_bytes(target_bytes), num_tables)

    scenario_root = OUTPUT_ROOT / scenario
    ensure_dirs(scenario_root)

    # Pre-generate schemas and estimates
    schema_entries = []
    for i in range(1, num_tables + 1):
        table_name = f"tbl_{i}"
        schema, create_sql = random_schema(table_name)
        schema_key = json.dumps([t for t in schema])
        est = estimate_row_size_for_schema(cache, schema_key, schema)
        schema_entries.append((table_name, schema, create_sql, est, schema_key))

    est_per_table = [e[3] for e in schema_entries]
    rows_alloc = compute_rows_per_table(target_bytes, num_tables, est_per_table)

    # Generate CSVs in parallel
    logging.info("Starting CSV generation with %d workers", GEN_WORKERS)
    results = []
    with Pool(processes=GEN_WORKERS) as p:
        for x in p.imap_unordered(partial(_generate_table_helper, scenario_root, rows_alloc), range(num_tables)):
            results.append(x)

    logging.info("CSV generation completed for %d tables", len(results))

    # Load CSVs into MySQL
    load_tasks = []
    for table_name, part_files, schema, rows_target in results:
        load_tasks.append((table_name, part_files, schema))

    logging.info("Starting LOAD DATA with %d workers", LOAD_WORKERS)
    successes = []
    with Pool(processes=LOAD_WORKERS) as p:
        for table_name, ok in p.imap_unordered(_load_table_helper, load_tasks):
            successes.append((table_name, ok))
            logging.info("Loaded %s: %s", table_name, ok)

    conn = mysql_connect()
    try:
        summary, total_bytes, total_rows = measure_db_size_and_row_counts(conn, DB_CONFIG["database"])
    finally:
        conn.close()

    logging.info("Final DB size: %s across %d rows", human_readable_bytes(total_bytes), total_rows)

    return summary, total_bytes, total_rows


def run_scenario(scenario: str):
    cache = load_cache()
    attempt = 0
    target_gb = SCENARIOS[scenario]["target_gb"]
    
    # Ensure target database exists before attempting any operations
    try:
        ensure_database_exists()
    except Exception:
        logging.error("Unable to ensure database exists; aborting")
        raise
    
    while attempt < MAX_RERUN_ATTEMPTS:
        attempt += 1
        logging.info("--- Attempt %d / %d ---", attempt, MAX_RERUN_ATTEMPTS)

        # Purge prior tables to ensure a clean run
        conn = mysql_connect()
        try:
            purge_tables(conn, prefix='tbl_')
        finally:
            conn.close()

        # run the generation+load once
        summary, total_bytes, total_rows = run_once(scenario, cache)

        # measure
        final_gb = total_bytes / (1024 ** 3)
        diff_pct = abs(final_gb - target_gb) / target_gb * 100
        logging.info("Attempt %d final size: %.2f GB (target %.2f GB) diff %.2f%%", attempt, final_gb, target_gb, diff_pct)

        # update cache with measured avg row size if we have rows
        if total_rows > 0:
            measured_avg_row = total_bytes / total_rows
            cache['measured_avg_row'] = measured_avg_row
            save_cache(cache)
            logging.info("Updated cache measured_avg_row = %f bytes", measured_avg_row)

        if diff_pct <= TARGET_TOLERANCE_PCT:
            logging.info("Target reached within ±%s%% after %d attempts", TARGET_TOLERANCE_PCT, attempt)
            break

        # If not reached, adjust estimated target using measured avg row size and re-run
        if total_bytes == 0:
            scale = 2.0
        else:
            scale = (target_gb * (1024 ** 3)) / total_bytes
        # We limit extreme scaling
        scale = max(0.5, min(scale, 5.0))
        # Apply scale factor to the scenario's target_gb for the next attempt
        old_target = SCENARIOS[scenario]['target_gb']
        SCENARIOS[scenario]['target_gb'] = old_target * scale
        logging.info("Adjusting scenario target from %.2f GB to %.2f GB for next attempt (scale %.3f)", old_target, SCENARIOS[scenario]['target_gb'], scale)

    else:
        logging.warning("Max attempts reached (%d) and target not reached within tolerance", MAX_RERUN_ATTEMPTS)

    return {
        'summary': summary,
        'total_bytes': total_bytes,
        'total_rows': total_rows,
        'attempts': attempt,
    }


# --------------------------- ENTRY POINT ---------------------------


def parse_args():
    p = argparse.ArgumentParser(description="Generate CSVs and bulk load into Azure MySQL Flexible Server")
    p.add_argument("--scenario", choices=SCENARIOS.keys(), default="test", help="Which scenario to run")
    return p.parse_args()


def main():
    args = parse_args()
    setup_logging(args.scenario)
    start = time.time()
    try:
        result = run_scenario(args.scenario)
        logging.info("Scenario completed. Result: %s", {"total_bytes": result['total_bytes'], "total_rows": result['total_rows'], "attempts": result['attempts']})
    except Exception:
        logging.exception("Scenario failed")
    finally:
        dur = time.time() - start
        logging.info("Total elapsed time: %s seconds", int(dur))


if __name__ == "__main__":
    main()
