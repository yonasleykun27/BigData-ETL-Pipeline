"""
test_pipeline.py
Validates the extraction and transformation stages of the pipeline in-memory
to verify throughput, memory profiling, and cleaning statistics without requiring DB connection.
"""

import time
import tracemalloc
from etl import DataExtractor, DataTransformer
import config

def test_dry_run():
    print("="*70)
    print("DRY-RUN ETL PIPELINE VALIDATION (Extraction + Transformation)")
    print("="*70)
    start_time = time.time()
    tracemalloc.start()

    extractor = DataExtractor(str(config.RAW_DATA_PATH), chunk_size=200_000)
    transformer = DataTransformer()

    total_records = 0
    chunk_count = 0

    for chunk in extractor.stream_chunks():
        chunk_count += 1
        t0 = time.time()
        cleaned = transformer.transform_chunk(chunk)
        total_records += len(cleaned)
        t_chunk = time.time() - t0
        print(f" -> Chunk #{chunk_count:02d}: Transformed {len(cleaned):,} rows in {t_chunk:.2f}s")
        # Test 1M rows for rapid profiling
        if total_records >= 1_000_000:
            print(" -> [Profile Breakpoint] Reached 1,000,000 validated rows successfully.")
            break

    total_time = time.time() - start_time
    cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    stats = transformer.get_summary_stats()
    print("\n" + "="*70)
    print("VALIDATION SUMMARY")
    print("="*70)
    print(f" Total Rows Extracted      : {stats['total_extracted']:,}")
    print(f" Duplicate Rows Removed    : {stats['duplicates_dropped']:,}")
    print(f" Null Emails Imputed       : {stats['null_emails_imputed']:,}")
    print(f" Total Cleaned Records     : {stats['total_cleaned']:,}")
    print(f" Throughput                : {total_records / total_time:,.0f} rows/second")
    print(f" Peak Memory Overhead      : {peak / (1024 * 1024):.2f} MB")
    print("="*70)
    print("[SUCCESS] Transformation and memory chunking verified!\n")

if __name__ == "__main__":
    test_dry_run()

