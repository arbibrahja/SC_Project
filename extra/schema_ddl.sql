-- =============================================================================
-- OLAP Intelligence Platform — Star Schema DDL
-- Database: SQLite (ANSI-compatible with PostgreSQL / DuckDB)
-- =============================================================================

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- Time Dimension
-- Hierarchy: Year → Quarter → Month → Day
CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER PRIMARY KEY,   -- surrogate key (YYYYMMDD format)
    full_date    TEXT    NOT NULL,      -- ISO date string e.g. '2024-03-15'
    year         INTEGER NOT NULL,      -- e.g. 2024
    quarter      TEXT    NOT NULL,      -- e.g. 'Q1', 'Q2', 'Q3', 'Q4'
    quarter_num  INTEGER NOT NULL,      -- e.g. 1, 2, 3, 4
    month        INTEGER NOT NULL,      -- e.g. 1–12
    month_name   TEXT    NOT NULL,      -- e.g. 'January'
    week_of_year INTEGER NOT NULL,      -- ISO week number 1–53
    day_of_week  INTEGER NOT NULL,      -- 0=Monday … 6=Sunday
    is_weekend   INTEGER NOT NULL       -- 0=weekday, 1=weekend
);

-- Geography Dimension
-- Hierarchy: Region → Country
CREATE TABLE IF NOT EXISTS dim_geography (
    geo_key INTEGER PRIMARY KEY,
    region  TEXT NOT NULL,              -- e.g. 'North America', 'Europe'
    country TEXT NOT NULL,              -- e.g. 'United States', 'Germany'
    UNIQUE(region, country)
);

-- Product Dimension
-- Hierarchy: Category → Subcategory
CREATE TABLE IF NOT EXISTS dim_product (
    product_key INTEGER PRIMARY KEY,
    category    TEXT NOT NULL,          -- e.g. 'Electronics', 'Furniture'
    subcategory TEXT NOT NULL,          -- e.g. 'Laptops', 'Chairs'
    UNIQUE(category, subcategory)
);

-- Customer Dimension
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key     INTEGER PRIMARY KEY,
    customer_segment TEXT NOT NULL UNIQUE  -- 'Consumer', 'Corporate', 'Home Office'
);

-- =============================================================================
-- FACT TABLE (centre of the star)
-- =============================================================================

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id       INTEGER PRIMARY KEY,
    order_id      TEXT    NOT NULL,

    -- Foreign keys to dimension tables
    date_key      INTEGER NOT NULL REFERENCES dim_date(date_key),
    geo_key       INTEGER NOT NULL REFERENCES dim_geography(geo_key),
    product_key   INTEGER NOT NULL REFERENCES dim_product(product_key),
    customer_key  INTEGER NOT NULL REFERENCES dim_customer(customer_key),

    -- Measures
    quantity      INTEGER NOT NULL,     -- units sold
    unit_price    REAL    NOT NULL,     -- price per unit ($)
    revenue       REAL    NOT NULL,     -- quantity × unit_price
    cost          REAL    NOT NULL,     -- cost of goods sold
    profit        REAL    NOT NULL,     -- revenue − cost
    profit_margin REAL    NOT NULL      -- profit / revenue × 100 (%)
);

-- =============================================================================
-- INDEXES (for fast aggregations across dimensions)
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_fact_date     ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_geo      ON fact_sales(geo_key);
CREATE INDEX IF NOT EXISTS idx_fact_product  ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales(customer_key);

-- =============================================================================
-- CONVENIENCE VIEW (denormalised, ready for OLAP queries)
-- =============================================================================

CREATE VIEW IF NOT EXISTS v_sales_full AS
SELECT
    f.order_id,
    d.full_date        AS order_date,
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
FROM  fact_sales    f
JOIN  dim_date      d ON f.date_key     = d.date_key
JOIN  dim_geography g ON f.geo_key      = g.geo_key
JOIN  dim_product   p ON f.product_key  = p.product_key
JOIN  dim_customer  c ON f.customer_key = c.customer_key;

-- =============================================================================
-- EXAMPLE OLAP QUERIES
-- =============================================================================

-- 1. Total revenue by region (Slice + Aggregate)
-- SELECT g.region, ROUND(SUM(f.revenue),2) AS total_revenue
-- FROM fact_sales f JOIN dim_geography g ON f.geo_key = g.geo_key
-- GROUP BY g.region ORDER BY total_revenue DESC;

-- 2. YoY revenue comparison (Roll-Up + Compare)
-- SELECT d.year, ROUND(SUM(f.revenue),2) AS total_revenue
-- FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_key
-- GROUP BY d.year ORDER BY d.year;

-- 3. Drill-down: Q4 2024 by month (Drill-Down)
-- SELECT d.month_name, ROUND(SUM(f.revenue),2) AS revenue
-- FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_key
-- WHERE d.year = 2024 AND d.quarter = 'Q4'
-- GROUP BY d.month, d.month_name ORDER BY d.month;

-- 4. Dice: Electronics in Europe
-- SELECT p.subcategory, ROUND(SUM(f.revenue),2) AS revenue
-- FROM fact_sales f
-- JOIN dim_product p ON f.product_key = p.product_key
-- JOIN dim_geography g ON f.geo_key = g.geo_key
-- WHERE p.category = 'Electronics' AND g.region = 'Europe'
-- GROUP BY p.subcategory ORDER BY revenue DESC;

-- 5. Top 5 countries by profit
-- SELECT g.country, ROUND(SUM(f.profit),2) AS total_profit
-- FROM fact_sales f JOIN dim_geography g ON f.geo_key = g.geo_key
-- GROUP BY g.country ORDER BY total_profit DESC LIMIT 5;
