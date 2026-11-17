"""
etl/transform.py
Modular Data Transformation & Cleaning Engine.
Performs data hygiene, type casting, missing value imputation, text normalization,
anomaly filtering, and deduplication for enterprise e-commerce transactions.
"""
import logging
import pandas as pd
import numpy as np
logger = logging.getLogger(__name__)
class DataTransformer:
    """
    Transforms raw transactional chunks into analytics-ready data.
    Implements robust validation rules and memory-efficient vector operations.
    """

    def __init__(self):
        self.seen_transaction_ids = set()
        self.stats = {
            "total_extracted": 0,
            "duplicates_dropped": 0,
            "null_emails_imputed": 0,
            "whitespace_cleaned": 0,
            "invalid_records_filtered": 0,
            "total_cleaned": 0
        }

    def transform_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans and normalizes a single chunk of data.
        """
        initial_count = len(df)
        self.stats["total_extracted"] += initial_count

        # 1. Deduplication (within chunk & against historical session cache)
        df = df.drop_duplicates(subset=["transaction_id"]).copy()
        
        # Cross-chunk deduplication check
        if self.seen_transaction_ids:
            is_new = ~df["transaction_id"].isin(self.seen_transaction_ids)
            dups_cross = len(df) - is_new.sum()
            self.stats["duplicates_dropped"] += (initial_count - len(df)) + dups_cross
            df = df[is_new]
        else:
            self.stats["duplicates_dropped"] += (initial_count - len(df))

        # Record new IDs into history
        self.seen_transaction_ids.update(df["transaction_id"])

        # 2. String Hygiene: Strip whitespace and normalize casing
        str_columns = [
            "customer_name", "customer_country", "customer_city",
            "product_category", "product_name", "payment_method",
            "order_status", "shipping_carrier"
        ]
        for col in str_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Standardize categorical casing
        df["order_status"] = df["order_status"].str.title()
        df["payment_method"] = df["payment_method"].str.title()
        df["product_category"] = df["product_category"].str.title()

        # 3. Handling Missing Values (Null Imputation)
        null_emails = df["customer_email"].isna() | (df["customer_email"].str.strip() == "")
        self.stats["null_emails_imputed"] += null_emails.sum()
        df.loc[null_emails, "customer_email"] = df.loc[null_emails, "customer_id"].str.lower() + "@guest.store.internal"

        # 4. Strict Type Casting
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype("int32")
        df["discount_rate"] = pd.to_numeric(df["discount_rate"], errors="coerce").fillna(0.0)
        df["tax_amount"] = pd.to_numeric(df["tax_amount"], errors="coerce").fillna(0.0)
        df["shipping_cost"] = pd.to_numeric(df["shipping_cost"], errors="coerce").fillna(0.0)
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")

        # Datetime conversion
        df["transaction_timestamp"] = pd.to_datetime(df["transaction_timestamp"], errors="coerce")

        # 5. Anomaly Detection and Business Rule Validation
        valid_mask = (
            (df["unit_price"] >= 0) &
            (df["quantity"] > 0) &
            (df["discount_rate"] >= 0.0) & (df["discount_rate"] <= 1.0) &
            (df["transaction_timestamp"].notna())
        )
        invalid_rows = len(df) - valid_mask.sum()
        if invalid_rows > 0:
            self.stats["invalid_records_filtered"] += invalid_rows
            df = df[valid_mask].copy()

        # Re-compute / guarantee financial total consistency: total = (price * qty * (1-disc)) + tax + shipping
        subtotal = np.round(df["unit_price"] * df["quantity"] * (1.0 - df["discount_rate"]), 2)
        df["total_amount"] = np.round(subtotal + df["tax_amount"] + df["shipping_cost"], 2)

        self.stats["total_cleaned"] += len(df)
        return df

    def get_summary_stats(self) -> dict:
        """Returns cumulative transformation metrics."""
        return self.stats

