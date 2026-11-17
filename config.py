"""
config.py
Configuration settings for the Big Data ETL Pipeline.
Loads database connection parameters and pipeline execution variables.
"""

import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_PATH = DATA_DIR / "raw" / "ecommerce_transactions_raw.csv"
SCHEMA_SQL_PATH = BASE_DIR / "database" / "schema.sql"
VERIFY_SQL_PATH = BASE_DIR / "database" / "verify.sql"

# Load .env file (with built-in fallback parser if python-dotenv is not installed)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

env_path = BASE_DIR / ".env"
if env_path.exists():
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

# ETL Processing Parameters
CHUNK_SIZE = 100_000  # Stream 100,000 records per chunk for memory efficiency
LOG_LEVEL = "INFO"

# Database Configuration (Defaults with environment variable overrides)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_dw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

def get_database_url(use_default_db=False):
    """Returns the SQLAlchemy connection URI."""
    target_db = "postgres" if use_default_db else DB_NAME
    return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{target_db}"
