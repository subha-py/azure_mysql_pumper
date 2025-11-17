#!/usr/bin/env python3
import os
import mysql.connector
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

HOST = "sbera-aurora-mysql.cluster-crtevtvwnjg4.us-west-1.rds.amazonaws.com"
DB_NAME = "synthetic_db"

def get_mysql_connection(database=None):
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", HOST),
        user=os.getenv("MYSQL_USER", "admin"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=database
    )

def delete_all_tables():
    """Delete all tables from the synthetic_db database."""
    conn = get_mysql_connection(DB_NAME)
    cur = conn.cursor()
    
    # Get all table names
    cur.execute("SHOW TABLES")
    tables = [row[0] for row in cur.fetchall()]
    
    if not tables:
        logging.info("No tables found in the database.")
        cur.close()
        conn.close()
        return
    
    logging.info(f"Found {len(tables)} tables to delete.")
    
    # Disable foreign key checks to avoid constraint issues
    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
    
    # Drop each table
    for table in tables:
        try:
            logging.info(f"Dropping table: {table}")
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
            logging.info(f"✓ Dropped table: {table}")
        except Exception as e:
            logging.error(f"✗ Failed to drop table {table}: {e}")
    
    # Re-enable foreign key checks
    cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    
    cur.close()
    conn.close()
    
    logging.info(f"✅ Successfully deleted all {len(tables)} tables from {DB_NAME} database.")

if __name__ == "__main__":
    import sys
    
    # Safety confirmation
    response = input(f"⚠️  WARNING: This will delete ALL tables from {DB_NAME} database on {HOST}.\nType 'yes' to confirm: ")
    
    if response.lower() == 'yes':
        delete_all_tables()
    else:
        logging.info("Operation cancelled.")
        sys.exit(0)
