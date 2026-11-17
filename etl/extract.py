"""
etl/extract.py
Modular Extraction Module for Large-Scale (2M+ Records) Datasets.
Employs Pandas chunking to maintain low memory overhead during data streaming.
"""

import logging
import os
from typing import Iterator
import pandas as pd

logger = logging.getLogger(__name__)

class DataExtractor:
    """
    Extracts raw transaction data from CSV sources using chunked streams.
    Ensures memory stability for multi-gigabyte files.
    """

    def __init__(self, file_path: str, chunk_size: int = 100_000):
        self.file_path = file_path
        self.chunk_size = chunk_size

        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Extraction failed: Source file '{self.file_path}' does not exist.")

    def stream_chunks(self) -> Iterator[pd.DataFrame]:
        """
        Yields DataFrames in sequential batches of size `self.chunk_size`.
        """
        logger.info(f"Initiating chunked extraction from: {self.file_path} (Chunk size: {self.chunk_size:,})")
        try:
            chunk_iterator = pd.read_csv(
                self.file_path,
                chunksize=self.chunk_size,
                low_memory=False,
                dtype={
                    "transaction_id": "string",
                    "order_id": "string",
                    "customer_id": "string",
                    "customer_name": "string",
                    "customer_email": "string",
                    "customer_country": "string",
                    "customer_city": "string",
                    "product_id": "string",
                    "product_category": "string",
                    "product_name": "string",
                    "payment_method": "string",
                    "order_status": "string",
                    "shipping_carrier": "string"
                }
            )

            for idx, chunk in enumerate(chunk_iterator, start=1):
                logger.debug(f"Extracted Chunk #{idx} ({len(chunk):,} rows)")
                yield chunk

        except Exception as e:
            logger.error(f"Error encountered during chunked extraction: {e}")
            raise

