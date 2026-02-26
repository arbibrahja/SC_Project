"""
Global Retail Sales Dataset Generator
Generates 10,000 transactions from Jan 2022 - Dec 2024
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

random.seed(42)
np.random.seed(42)

REGIONS = {
    "North America": ["United States", "Canada", "Mexico"],
    "Europe": ["Germany", "United Kingdom", "France", "Spain", "Italy"],
    "Asia Pacific": ["Japan", "Australia", "China", "India", "Singapore"],
    "Latin America": ["Brazil", "Argentina", "Colombia", "Chile"],
}

PRODUCTS = {
    "Electronics": {
        "Laptops": (800, 2500),
        "Smartphones": (400, 1200),
        "Tablets": (300, 900),
        "Headphones": (50, 400),
        "Monitors": (200, 800),
    },
    "Furniture": {
        "Office Chairs": (150, 800),
        "Desks": (200, 1200),
        "Bookcases": (100, 500),
        "Storage": (80, 400),
    },
    "Office Supplies": {
        "Paper & Notebooks": (10, 80),
        "Pens & Pencils": (5, 40),
        "Binders & Files": (15, 60),
        "Printer Supplies": (20, 150),
    },
    "Clothing": {
        "Shirts": (30, 150),
        "Pants": (40, 200),
        "Jackets": (80, 400),
        "Accessories": (15, 100),
    },
}

SEGMENTS = ["Consumer", "Corporate", "Home Office"]

MARGIN_RATES = {
    "Electronics": 0.22,
    "Furniture": 0.35,
    "Office Supplies": 0.45,
    "Clothing": 0.55,
}


def generate_date(start="2022-01-01", end="2024-12-31"):
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    delta = end_dt - start_dt
    random_days = random.randint(0, delta.days)
    return start_dt + timedelta(days=random_days)


def generate_dataset(n=10000):
    rows = []
    order_id = 10001

    for _ in range(n):
        date = generate_date()
        region = random.choice(list(REGIONS.keys()))
        country = random.choice(REGIONS[region])
        category = random.choice(list(PRODUCTS.keys()))
        subcategory = random.choice(list(PRODUCTS[category].keys()))
        price_range = PRODUCTS[category][subcategory]
        unit_price = round(random.uniform(*price_range), 2)
        quantity = random.randint(1, 10)
        revenue = round(unit_price * quantity, 2)
        margin = MARGIN_RATES[category] + random.uniform(-0.05, 0.05)
        cost = round(revenue * (1 - margin), 2)
        profit = round(revenue - cost, 2)
        profit_margin = round(profit / revenue * 100, 2)
        segment = random.choice(SEGMENTS)

        rows.append({
            "order_id": f"ORD-{order_id}",
            "order_date": date.strftime("%Y-%m-%d"),
            "year": date.year,
            "quarter": f"Q{(date.month - 1) // 3 + 1}",
            "month": date.month,
            "month_name": date.strftime("%B"),
            "region": region,
            "country": country,
            "category": category,
            "subcategory": subcategory,
            "customer_segment": segment,
            "quantity": quantity,
            "unit_price": unit_price,
            "revenue": revenue,
            "cost": cost,
            "profit": profit,
            "profit_margin": profit_margin,
        })
        order_id += 1

    df = pd.DataFrame(rows)
    df = df.sort_values("order_date").reset_index(drop=True)
    return df


if __name__ == "__main__":
    print("Generating dataset...")
    df = generate_dataset(10000)
    df.to_csv("global_retail_sales.csv", index=False)
    print(f"Generated {len(df)} records")
    print(df.head())
    print("\nSummary:")
    print(f"  Date range: {df['order_date'].min()} to {df['order_date'].max()}")
    print(f"  Total revenue: ${df['revenue'].sum():,.2f}")
    print(f"  Regions: {df['region'].unique()}")
    print(f"  Categories: {df['category'].unique()}")
