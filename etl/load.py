"""
etl/load.py
High-Performance Database Loader for PostgreSQL.
Uses PostgreSQL COPY FROM STDIN protocol via psycopg2 / SQLAlchemy raw connections
to achieve maximum bulk-insertion speeds for 2M+ records.
"""
import io
import logging
from sqlalchemy import create_engine, text
import pandas as pd
import psycopg2

logger = logging.getLogger(__name__)
class DataLoader:
    """
    Manages database connections, schema provisioning, and high-throughput loading.
    """

    def __init__(self, db_url: str, default_db_url: str = None, target_db_name: str = "ecommerce_dw"):
        self.db_url = db_url
        self.default_db_url = default_db_url
        self.target_db_name = target_db_name
        self.engine = None

    def initialize_database(self, schema_file_path: str):
        """
        Creates target database if absent, and provisions the schema, tables, and indexes.
        """
        logger.info(f"Connecting to database server...")
        
        # Connect to default DB to ensure target database exists
        if self.default_db_url:
            try:
                default_engine = create_engine(self.default_db_url, isolation_level="AUTOCOMMIT")
                with default_engine.connect() as conn:
                    result = conn.execute(
                        text(f"SELECT 1 FROM pg_database WHERE datname = '{self.target_db_name}'")
                    ).fetchone()
                    if not result:
                        logger.info(f"Database '{self.target_db_name}' not found. Creating database...")
                        conn.execute(text(f"CREATE DATABASE {self.target_db_name}"))
                        logger.info(f"Database '{self.target_db_name}' created successfully.")
                    else:
                        logger.info(f"Database '{self.target_db_name}' already exists.")
            except Exception as e:
                logger.warning(f"Could not verify/create database via default connection: {e}")

        # Connect to target database
        self.engine = create_engine(self.db_url)

        # Apply schema DDL via raw connection to handle multiple statements reliably
        logger.info(f"Applying schema definitions from '{schema_file_path}'...")
        with open(schema_file_path, "r", encoding="utf-8") as f:
            ddl_statements = f.read()

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                cursor.execute(ddl_statements)
            raw_conn.commit()
        finally:
            raw_conn.close()
        logger.info("Schema, tables, constraints, and indexes successfully provisioned.")

    def bulk_load_chunk(self, df: pd.DataFrame, table_name: str = "ecommerce_transactions") -> int:
        """
        Ultra-fast bulk load using PostgreSQL's COPY protocol through an in-memory buffer.
        """
        if self.engine is None:
            self.engine = create_engine(self.db_url)

        # Prepare in-memory CSV buffer
        buffer = io.StringIO()
        # Drop temporary audit column if present in dataframe
        cols_to_export = [
            "transaction_id", "order_id", "customer_id", "customer_name", "customer_email",
            "customer_country", "customer_city", "product_id", "product_category", "product_name",
            "unit_price", "quantity", "discount_rate", "tax_amount", "shipping_cost",
            "total_amount", "payment_method", "order_status", "shipping_carrier", "transaction_timestamp"
        ]
        
        export_df = df[[c for c in cols_to_export if c in df.columns]]
        export_df.to_csv(buffer, index=False, header=True, sep="\t")
        buffer.seek(0)

        # Obtain raw psycopg2 connection to run copy_expert
        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                columns_formatted = ", ".join([f'"{c}"' for c in export_df.columns])
                copy_sql = f'COPY "{table_name}" ({columns_formatted}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER E\'\\t\')'
                cursor.copy_expert(sql=copy_sql, file=buffer)
            raw_conn.commit()
            return len(export_df)
        finally:
            raw_conn.close()

    def populate_dimensions(self):
        """
        Populates normalized dimension tables (dim_customers, dim_products)
        from loaded transactions.
        """
        logger.info("Populating normalized dimension tables (dim_customers, dim_products)...")
        cust_sql = text("""
            INSERT INTO dim_customers (customer_id, customer_name, customer_email, customer_country, customer_city, first_seen, last_seen)
            SELECT 
                customer_id,
                MAX(customer_name),
                MAX(customer_email),
                MAX(customer_country),
                MAX(customer_city),
                MIN(transaction_timestamp),
                MAX(transaction_timestamp)
            FROM ecommerce_transactions
            GROUP BY customer_id
            ON CONFLICT (customer_id) DO NOTHING;
        """)

        prod_sql = text("""
            INSERT INTO dim_products (product_id, product_name, product_category)
            SELECT 
                product_id,
                MAX(product_name),
                MAX(product_category)
            FROM ecommerce_transactions
            GROUP BY product_id
            ON CONFLICT (product_id) DO NOTHING;
        """)

        with self.engine.begin() as conn:
            conn.execute(cust_sql)
            conn.execute(prod_sql)
        logger.info("Dimension tables successfully populated.")

    def verify_total_rows(self, table_name: str = "ecommerce_transactions") -> int:
        """Returns total loaded row count."""
        with self.engine.connect() as conn:
            count = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()
        return count

