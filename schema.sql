-- ============================================================
-- OLAP Star Schema - Global Retail Sales
-- Compatible with DuckDB and PostgreSQL
-- ============================================================

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE NOT NULL,
    year            INTEGER NOT NULL,
    quarter         VARCHAR(2) NOT NULL,  -- Q1, Q2, Q3, Q4
    quarter_num     INTEGER NOT NULL,     -- 1, 2, 3, 4
    month           INTEGER NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    week_of_year    INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

-- Dimension: Geography
CREATE TABLE IF NOT EXISTS dim_geography (
    geo_key     INTEGER PRIMARY KEY,
    region      VARCHAR(50) NOT NULL,
    country     VARCHAR(100) NOT NULL,
    UNIQUE(region, country)
);

-- Dimension: Product
CREATE TABLE IF NOT EXISTS dim_product (
    product_key     INTEGER PRIMARY KEY,
    category        VARCHAR(100) NOT NULL,
    subcategory     VARCHAR(100) NOT NULL,
    UNIQUE(category, subcategory)
);

-- Dimension: Customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key        INTEGER PRIMARY KEY,
    customer_segment    VARCHAR(50) NOT NULL,
    UNIQUE(customer_segment)
);

-- Fact Table: Sales
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id         INTEGER PRIMARY KEY,
    order_id        VARCHAR(20) NOT NULL,
    date_key        INTEGER NOT NULL REFERENCES dim_date(date_key),
    geo_key         INTEGER NOT NULL REFERENCES dim_geography(geo_key),
    product_key     INTEGER NOT NULL REFERENCES dim_product(product_key),
    customer_key    INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    quantity        INTEGER NOT NULL,
    unit_price      DECIMAL(10,2) NOT NULL,
    revenue         DECIMAL(12,2) NOT NULL,
    cost            DECIMAL(12,2) NOT NULL,
    profit          DECIMAL(12,2) NOT NULL,
    profit_margin   DECIMAL(5,2) NOT NULL
);

-- Indexes for analytical performance
CREATE INDEX IF NOT EXISTS idx_fact_date    ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_geo     ON fact_sales(geo_key);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales(customer_key);

-- ============================================================
-- Analytical Views (convenience layer)
-- ============================================================

CREATE OR REPLACE VIEW v_sales_full AS
SELECT
    f.order_id,
    d.full_date      AS order_date,
    d.year,
    d.quarter,
    d.quarter_num,
    d.month,
    d.month_name,
    g.region,
    g.country,
    p.category,
    p.subcategory,
    c.customer_segment,
    f.quantity,
    f.unit_price,
    f.revenue,
    f.cost,
    f.profit,
    f.profit_margin
FROM fact_sales f
JOIN dim_date     d ON f.date_key     = d.date_key
JOIN dim_geography g ON f.geo_key     = g.geo_key
JOIN dim_product   p ON f.product_key = p.product_key
JOIN dim_customer  c ON f.customer_key = c.customer_key;
