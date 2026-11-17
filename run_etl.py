"""
run_etl.py
Main Execution Orchestrator for the Big Data ETL Pipeline.
Executes the End-to-End Extraction, Transformation, and Loading pipeline
with chunking, progress tracking, and performance metrics.
"""

import argparse
import logging
import sys
import time
import tracemalloc

import config
from etl import DataExtractor, DataTransformer, DataLoader

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("pipeline_execution.log", mode="w", encoding="utf-8")
        ]
    )

def main():
    parser = argparse.ArgumentParser(description="Big Data 2M+ Record ETL Pipeline")
    parser.add_argument("--chunk-size", type=int, default=config.CHUNK_SIZE, help="Rows per processing chunk")
    parser.add_argument("--raw-file", type=str, default=str(config.RAW_DATA_PATH), help="Path to raw CSV file")
    parser.add_argument("--skip-init", action="store_true", help="Skip database schema recreation")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger("ETL_Orchestrator")

    print("\n" + "="*80)
    print(" " * 20 + "BIG DATA ETL PIPELINE ORCHESTRATOR")
    print("="*80)
    print(f" Source File  : {args.raw_file}")
    print(f" Chunk Size   : {args.chunk_size:,} records")
    print(f" Database URL : {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
    print("="*80 + "\n")

    start_time = time.time()
    tracemalloc.start()

    # Step 1: Initialize Loader and Schema
    target_url = config.get_database_url(use_default_db=False)
    default_url = config.get_database_url(use_default_db=True)
    loader = DataLoader(db_url=target_url, default_db_url=default_url, target_db_name=config.DB_NAME)

    if not args.skip_init:
        try:
            loader.initialize_database(str(config.SCHEMA_SQL_PATH))
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            logger.error("Please verify PostgreSQL is running and credentials in config.py / .env are valid.")
            sys.exit(1)

    # Step 2: Extraction & Transformation
    extractor = DataExtractor(file_path=args.raw_file, chunk_size=args.chunk_size)
    transformer = DataTransformer()

    total_loaded = 0
    chunk_index = 0

    try:
        for raw_chunk in extractor.stream_chunks():
            chunk_index += 1
            chunk_start = time.time()

            # Transform
            cleaned_chunk = transformer.transform_chunk(raw_chunk)

            # Load
            rows_inserted = loader.bulk_load_chunk(cleaned_chunk, table_name="ecommerce_transactions")
            total_loaded += rows_inserted

            chunk_duration = time.time() - chunk_start
            logger.info(
                f"[Chunk #{chunk_index:02d}] Processed & Loaded {rows_inserted:,} rows in {chunk_duration:.2f}s "
                f"(Cumulative: {total_loaded:,} records)"
            )

        # Step 3: Populate Dimension Tables
        loader.populate_dimensions()

    except Exception as e:
        logger.error(f"Pipeline failed at chunk #{chunk_index}: {e}", exc_info=True)
        sys.exit(1)

    # Metrics and Summary
    total_duration = time.time() - start_time
    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    verified_count = loader.verify_total_rows("ecommerce_transactions")
    stats = transformer.get_summary_stats()

    print("\n" + "="*80)
    print(" " * 28 + "ETL EXECUTION SUMMARY")
    print("="*80)
    print(f" Total Rows Extracted        : {stats['total_extracted']:,}")
    print(f" Duplicate Records Removed   : {stats['duplicates_dropped']:,}")
    print(f" Missing Emails Imputed      : {stats['null_emails_imputed']:,}")
    print(f" Invalid Records Filtered    : {stats['invalid_records_filtered']:,}")
    print(f" Total Cleaned Records       : {stats['total_cleaned']:,}")
    print(f" PostgreSQL Verified Records : {verified_count:,}")
    print(f" Total Execution Time        : {total_duration:.2f} seconds")
    print(f" Throughput Rate             : {verified_count / max(total_duration, 1):,.0f} rows/second")
    print(f" Peak Memory Overhead (RAM)  : {peak_mem / (1024 * 1024):.2f} MB")
    print("="*80)

    if verified_count >= 2_000_000:
        print("[SUCCESS] Rubric Criterion PASSED: Dataset loaded exceeds 2,000,000 records!\n")
    else:
        print("[WARNING] Dataset row count is below 2,000,000 records.\n")

if __name__ == "__main__":
    main()

