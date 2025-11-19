"""
MOVIELENS 25M - SIMPLE RAW INGESTION (PANDAS CHUNKED -> POSTGRESQL)
===================================================================

PROJECT: Netflix / MovieLens Analytics
AUTHOR: Dipanshu Kumar
DATE: 19/10/2025

PURPOSE:
- Automated raw ingestion of CSV files into PostgreSQL using pandas chunking
- No data cleaning or transformation; preserves original headers and values
- Optimized chunk size for large MovieLens 25M files

DATA SOURCES:
- CSV files in ./data (e.g., genome-scores.csv, genome-tags.csv, links.csv, movies.csv, ratings.csv, tags.csv)
"""

import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine

# ==================== CONFIGURATION ====================
# Database configuration - REPLACE WITH YOUR CREDENTIALS
DB_USER = "your_postgres_username"
DB_PASS = "your_postgres_password" 
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "vendor_db"        # Target database

# File system configuration
DATA_DIR = "data"         # Source directory containing CSV files
LOG_DIR = "logs"          # Log directory for ingestion monitoring

# Performance tuning parameters (large files)
CHUNK_SIZE = 200_000
WRITE_METHOD = "multi"     # Batch insert method for optimal performance

# ==================== LOGGING SETUP ====================
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "ml25m_ingestion.log"),
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

# ==================== DATABASE ENGINE ====================
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ==================== HELPER FUNCTIONS ====================
def _safe_table_name(name: str) -> str:
    """
    Convert filename to SQL-safe table name without altering data.
    """
    base = os.path.splitext(name)[0]
    clean = "".join(ch.lower() if ch.isalnum() else "_" for ch in base).strip("_")
    while "__" in clean:
        clean = clean.replace("__", "_")
    return clean or "table_from_csv"

def ingest_csv_chunked(file_path: str, table_name: str):
    """
    Stream CSV files into PostgreSQL using chunked processing with no cleaning.
    - Preserves headers and values exactly as read.
    - Uses Python parsing engine to avoid C-engine tokenizer issues.
    """
    try:
        first = True
        rows_total = 0

        # Use Python engine with explicit delimiter; omit low_memory (not supported for python engine)
        reader = pd.read_csv(
            file_path,
            chunksize=CHUNK_SIZE,
            engine="python",
            sep=",",
        )

        for chunk in reader:
            # Do NOT modify column names or values; write as-is
            if_exists = "replace" if first else "append"
            chunk.to_sql(
                table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                method=WRITE_METHOD,
            )
            rows_total += len(chunk)
            first = False

        logging.info(f"✅ Ingested {rows_total} rows into '{table_name}' from {os.path.basename(file_path)}")
        print(f"✅ {os.path.basename(file_path)} → {table_name} ({rows_total} rows)")

    except Exception as e:
        logging.error(f"❌ Failed ingest for {file_path} → {table_name}: {e}")
        print(f"❌ Error: {file_path} → {table_name}: {e}")

def load_raw_data():
    """
    Main data ingestion controller function.
    """
    start = time.time()

    if not os.path.isdir(DATA_DIR):
        logging.error(f"Data folder not found: {DATA_DIR}")
        print(f"❌ Data folder not found: {DATA_DIR}")
        return

    files = [f for f in os.listdir(DATA_DIR) if f.lower().endswith(".csv")]
    if not files:
        logging.warning("No CSV files found in 'data/'")
        print("⚠️ No CSVs in data/")
        return

    for file in sorted(files):
        file_path = os.path.join(DATA_DIR, file)
        table_name = _safe_table_name(file)
        logging.info(f"📥 Ingesting {file} → table '{table_name}' (chunked)")
        print(f"📥 {file} → {table_name}")
        ingest_csv_chunked(file_path, table_name)

    mins = (time.time() - start) / 60
    logging.info("-- Ingestion Complete --")
    logging.info(f"Total Time Taken: {mins:.2f} minutes")
    print(f"🎉 Done in {mins:.2f} minutes")

# ==================== MAIN EXECUTION ====================
if __name__ == "__main__":
    load_raw_data()
