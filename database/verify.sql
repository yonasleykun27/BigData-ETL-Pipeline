-- ============================================================================
-- BIG DATA ETL PIPELINE: VERIFICATION & AUDIT SUITE
-- Verifies:
--   1. Total record volume >= 2,000,000 rows
--   2. Data integrity, null rate, and boundary constraints
--   3. Business intelligence analytical queries
--   4. Query performance execution plans (EXPLAIN ANALYZE)
-- ============================================================================

-- 1. VERIFY TOTAL RECORD COUNT (Must be >= 2,000,000)
SELECT 
    COUNT(*) AS total_records_loaded,
    CASE 
        WHEN COUNT(*) >= 2000000 THEN 'PASSED: Exceeds 2M Record Requirement'
        ELSE 'FAILED: Below 2M Requirement'
    END AS rubric_compliance_status
FROM ecommerce_transactions;

-- 2. INTEGRITY CHECKS: Verify Absence of Negative Amounts and Critical Nulls
SELECT 
    COUNT(*) FILTER (WHERE transaction_id IS NULL) AS null_txn_ids,
    COUNT(*) FILTER (WHERE total_amount < 0) AS invalid_negative_totals,
    COUNT(*) FILTER (WHERE unit_price < 0) AS invalid_negative_prices,
    COUNT(*) FILTER (WHERE quantity <= 0) AS invalid_quantities,
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS missing_customer_ids
FROM ecommerce_transactions;

-- 3. AUDIT DATA SUMMARY (Date Range, Min/Max/Avg Financials)
SELECT 
    MIN(transaction_timestamp) AS earliest_transaction,
    MAX(transaction_timestamp) AS latest_transaction,
    MIN(total_amount) AS min_total_amount,
    MAX(total_amount) AS max_total_amount,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(SUM(total_amount), 2) AS gross_revenue_all_time
FROM ecommerce_transactions;

-- 4. BUSINESS INTELLIGENCE QUERY 1: Revenue & Volume by Product Category
SELECT 
    product_category,
    COUNT(*) AS transaction_count,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM ecommerce_transactions
GROUP BY product_category
ORDER BY total_revenue DESC;

-- 5. BUSINESS INTELLIGENCE QUERY 2: Top 10 High-Performing Global Markets
SELECT 
    customer_country,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) AS order_volume,
    ROUND(SUM(total_amount), 2) AS total_sales_usd
FROM ecommerce_transactions
GROUP BY customer_country
ORDER BY total_sales_usd DESC
LIMIT 10;

-- 6. BUSINESS INTELLIGENCE QUERY 3: Payment Method Distribution & Completion Rate
SELECT 
    payment_method,
    COUNT(*) AS usage_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS share_percentage,
    COUNT(*) FILTER (WHERE order_status = 'Completed') AS completed_orders,
    ROUND(COUNT(*) FILTER (WHERE order_status = 'Completed') * 100.0 / COUNT(*), 2) AS completion_rate_pct
FROM ecommerce_transactions
GROUP BY payment_method
ORDER BY usage_count DESC;

-- 7. PERFORMANCE BENCHMARK: Query Plan using B-Tree Index on Timestamp
EXPLAIN ANALYZE
SELECT 
    DATE_TRUNC('month', transaction_timestamp) AS sales_month,
    COUNT(*) AS monthly_orders,
    ROUND(SUM(total_amount), 2) AS monthly_revenue
FROM ecommerce_transactions
WHERE transaction_timestamp >= '2025-01-01' AND transaction_timestamp < '2026-01-01'
GROUP BY sales_month
ORDER BY sales_month;

-- 8. FINAL ANALYTICAL VIEW PREVIEW: Star Schema Consolidated Reporting View
SELECT 
    transaction_id,
    order_id,
    customer_name,
    customer_country,
    product_category,
    product_name,
    unit_price,
    quantity,
    total_amount,
    order_status,
    transaction_timestamp
FROM vw_ecommerce_final_summary
ORDER BY transaction_timestamp DESC
LIMIT 10;


