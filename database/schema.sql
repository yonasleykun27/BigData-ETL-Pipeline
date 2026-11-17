-- ============================================================================
-- BIG DATA ETL PIPELINE: POSTGRESQL RELATIONAL SCHEMA DEFINITION
-- Business Scenario: Global Omnichannel E-Commerce & Retail Transactions
-- Target Volume: 2,000,000+ Records
-- Compliance: 3NF / Dimensional Modeling with Constraints & Performance Indexes
-- ============================================================================

-- Drop tables if re-initializing
DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS ecommerce_transactions CASCADE;

-- ----------------------------------------------------------------------------
-- 1. Consolidated High-Performance Transactions Table (Fact Staging & Warehouse)
-- ----------------------------------------------------------------------------
CREATE TABLE ecommerce_transactions (
    transaction_id VARCHAR(36) PRIMARY KEY,
    order_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    customer_email VARCHAR(150),
    customer_country VARCHAR(50) NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    product_category VARCHAR(50) NOT NULL,
    product_name VARCHAR(150) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    discount_rate NUMERIC(4, 2) NOT NULL DEFAULT 0.00 CHECK (discount_rate >= 0.00 AND discount_rate <= 1.00),
    tax_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00 CHECK (tax_amount >= 0),
    shipping_cost NUMERIC(10, 2) NOT NULL DEFAULT 0.00 CHECK (shipping_cost >= 0),
    total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    payment_method VARCHAR(30) NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    shipping_carrier VARCHAR(50) NOT NULL,
    transaction_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 2. Performance B-Tree Indexes (Optimized for 2M+ Record Analytical Queries)
-- ----------------------------------------------------------------------------
-- Time-series range queries (e.g. monthly revenue, daily trends)
CREATE INDEX idx_txn_timestamp ON ecommerce_transactions (transaction_timestamp DESC);

-- Customer aggregation queries (e.g. Customer Lifetime Value, order history)
CREATE INDEX idx_txn_customer ON ecommerce_transactions (customer_id);

-- Product category and SKU aggregations (e.g. Top selling categories)
CREATE INDEX idx_txn_product ON ecommerce_transactions (product_id, product_category);

-- Operational status filtering (e.g. Cancelled, Refunded, Completed orders)
CREATE INDEX idx_txn_status ON ecommerce_transactions (order_status);

-- Geographic regional analysis (e.g. sales by country and city)
CREATE INDEX idx_txn_country_city ON ecommerce_transactions (customer_country, customer_city);

-- Composite index for fast customer timeline filtering
CREATE INDEX idx_txn_cust_time ON ecommerce_transactions (customer_id, transaction_timestamp DESC);

-- ----------------------------------------------------------------------------
-- 3. Normalized Dimension Tables (Demonstrating 3NF / Dimensional Modeling)
-- ----------------------------------------------------------------------------
CREATE TABLE dim_customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    customer_email VARCHAR(150),
    customer_country VARCHAR(50) NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    first_seen TIMESTAMP WITHOUT TIME ZONE,
    last_seen TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE dim_products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    product_category VARCHAR(50) NOT NULL
);

-- Performance indexes for dimensions
CREATE INDEX idx_dim_cust_country ON dim_customers (customer_country);
CREATE INDEX idx_dim_prod_cat ON dim_products (product_category);

-- ----------------------------------------------------------------------------
-- 4. Analytical Reporting View (Unified Star Schema Final Reporting View)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_ecommerce_final_summary AS
SELECT 
    t.transaction_id,
    t.order_id,
    t.customer_id,
    c.customer_name,
    c.customer_email,
    c.customer_country,
    c.customer_city,
    t.product_id,
    p.product_name,
    p.product_category,
    t.unit_price,
    t.quantity,
    t.discount_rate,
    t.tax_amount,
    t.shipping_cost,
    t.total_amount,
    t.payment_method,
    t.order_status,
    t.shipping_carrier,
    t.transaction_timestamp
FROM ecommerce_transactions t
LEFT JOIN dim_customers c ON t.customer_id = c.customer_id
LEFT JOIN dim_products p ON t.product_id = p.product_id;

COMMENT ON TABLE ecommerce_transactions IS 'Consolidated 2M+ E-Commerce transaction records with full audit trail';
COMMENT ON COLUMN ecommerce_transactions.transaction_id IS 'Unique UUID/Alpha-numeric transaction key';
COMMENT ON COLUMN ecommerce_transactions.total_amount IS 'Calculated net transaction total including tax and shipping minus discount';
COMMENT ON VIEW vw_ecommerce_final_summary IS 'Final consolidated analytical reporting view joining transactions with dimensions';

