"""
Core database module.
Uses SQLite (stdlib) so the project works without DuckDB installed.
All SQL is ANSI-compatible and will also run on PostgreSQL/DuckDB.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional

DB_PATH = Path("olap.db")
CSV_PATH = Path("data") / "global_retail_sales.csv"


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or str(DB_PATH)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def init_schema(con: sqlite3.Connection):
    con.executescript("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key     INTEGER PRIMARY KEY,
        full_date    TEXT NOT NULL,
        year         INTEGER NOT NULL,
        quarter      TEXT NOT NULL,
        quarter_num  INTEGER NOT NULL,
        month        INTEGER NOT NULL,
        month_name   TEXT NOT NULL,
        week_of_year INTEGER NOT NULL,
        day_of_week  INTEGER NOT NULL,
        is_weekend   INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_geography (
        geo_key INTEGER PRIMARY KEY,
        region  TEXT NOT NULL,
        country TEXT NOT NULL,
        UNIQUE(region, country)
    );

    CREATE TABLE IF NOT EXISTS dim_product (
        product_key INTEGER PRIMARY KEY,
        category    TEXT NOT NULL,
        subcategory TEXT NOT NULL,
        UNIQUE(category, subcategory)
    );

    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_key     INTEGER PRIMARY KEY,
        customer_segment TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS fact_sales (
        sale_id      INTEGER PRIMARY KEY,
        order_id     TEXT NOT NULL,
        date_key     INTEGER NOT NULL REFERENCES dim_date(date_key),
        geo_key      INTEGER NOT NULL REFERENCES dim_geography(geo_key),
        product_key  INTEGER NOT NULL REFERENCES dim_product(product_key),
        customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),
        quantity     INTEGER NOT NULL,
        unit_price   REAL NOT NULL,
        revenue      REAL NOT NULL,
        cost         REAL NOT NULL,
        profit       REAL NOT NULL,
        profit_margin REAL NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_fact_date     ON fact_sales(date_key);
    CREATE INDEX IF NOT EXISTS idx_fact_geo      ON fact_sales(geo_key);
    CREATE INDEX IF NOT EXISTS idx_fact_product  ON fact_sales(product_key);
    CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales(customer_key);

    CREATE VIEW IF NOT EXISTS v_sales_full AS
    SELECT
        f.order_id,
        d.full_date   AS order_date,
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
    """)
    con.commit()


def run_etl(con: sqlite3.Connection, csv_path: Optional[str] = None):
    """Load CSV into star schema tables."""
    path = csv_path or str(CSV_PATH)
    print(f"Loading {path}...")
    df = pd.read_csv(path)
    df["full_date"] = pd.to_datetime(df["order_date"])

    # --- dim_date ---
    dates = df[["order_date", "year", "quarter", "month", "month_name"]].drop_duplicates()
    dates = dates.copy()
    dates["full_date"] = pd.to_datetime(dates["order_date"])
    dates["date_key"] = dates["full_date"].dt.strftime("%Y%m%d").astype(int)
    dates["quarter_num"] = dates["quarter"].str[1].astype(int)
    dates["week_of_year"] = dates["full_date"].dt.isocalendar().week.astype(int)
    dates["day_of_week"] = dates["full_date"].dt.dayofweek
    dates["is_weekend"] = (dates["day_of_week"] >= 5).astype(int)
    for _, r in dates.iterrows():
        con.execute("""
            INSERT OR IGNORE INTO dim_date
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (r.date_key, r.order_date, r.year, r.quarter, r.quarter_num,
              r.month, r.month_name, r.week_of_year, r.day_of_week, r.is_weekend))

    # --- dim_geography ---
    geos = df[["region", "country"]].drop_duplicates().reset_index(drop=True)
    geo_map = {}
    for i, r in geos.iterrows():
        con.execute("INSERT OR IGNORE INTO dim_geography(region,country) VALUES(?,?)",
                    (r.region, r.country))
        row = con.execute("SELECT geo_key FROM dim_geography WHERE region=? AND country=?",
                          (r.region, r.country)).fetchone()
        geo_map[(r.region, r.country)] = row[0]

    # --- dim_product ---
    prods = df[["category", "subcategory"]].drop_duplicates().reset_index(drop=True)
    prod_map = {}
    for i, r in prods.iterrows():
        con.execute("INSERT OR IGNORE INTO dim_product(category,subcategory) VALUES(?,?)",
                    (r.category, r.subcategory))
        row = con.execute("SELECT product_key FROM dim_product WHERE category=? AND subcategory=?",
                          (r.category, r.subcategory)).fetchone()
        prod_map[(r.category, r.subcategory)] = row[0]

    # --- dim_customer ---
    custs = df["customer_segment"].unique()
    cust_map = {}
    for seg in custs:
        con.execute("INSERT OR IGNORE INTO dim_customer(customer_segment) VALUES(?)", (seg,))
        row = con.execute("SELECT customer_key FROM dim_customer WHERE customer_segment=?",
                          (seg,)).fetchone()
        cust_map[seg] = row[0]

    con.commit()

    # --- fact_sales ---
    date_lookup = {r.order_date: int(pd.Timestamp(r.order_date).strftime("%Y%m%d"))
                   for _, r in dates.iterrows()}

    records = []
    for i, r in df.iterrows():
        records.append((
            i + 1,
            r.order_id,
            date_lookup[r.order_date],
            geo_map[(r.region, r.country)],
            prod_map[(r.category, r.subcategory)],
            cust_map[r.customer_segment],
            int(r.quantity),
            float(r.unit_price),
            float(r.revenue),
            float(r.cost),
            float(r.profit),
            float(r.profit_margin),
        ))

    con.executemany("""
        INSERT OR IGNORE INTO fact_sales VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, records)
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    print(f"ETL complete. {n} records in fact_sales.")


def query_df(sql: str, params=None, db_path: Optional[str] = None) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame."""
    con = get_connection(db_path)
    try:
        cur = con.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame([dict(zip(cols, r)) for r in rows])
    finally:
        con.close()


def ensure_db(db_path: Optional[str] = None, csv_path: Optional[str] = None):
    """Initialize and populate the database if needed."""
    path = db_path or str(DB_PATH)
    con = get_connection(path)
    init_schema(con)
    count = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    if count == 0:
        run_etl(con, csv_path)
    else:
        print(f"Database ready ({count} records).")
    con.close()


if __name__ == "__main__":
    ensure_db()
    result = query_df("""
        SELECT g.region, ROUND(SUM(f.revenue),2) AS total_revenue
        FROM fact_sales f
        JOIN dim_geography g ON f.geo_key = g.geo_key
        GROUP BY g.region ORDER BY total_revenue DESC
    """)
    print(result)
