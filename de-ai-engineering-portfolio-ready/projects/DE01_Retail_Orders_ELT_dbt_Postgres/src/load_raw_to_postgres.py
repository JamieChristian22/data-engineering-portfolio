from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import sqlalchemy as sa

def _engine(pg: dict) -> sa.Engine:
    url = sa.URL.create(
        "postgresql+psycopg2",
        username=pg["user"],
        password=pg["password"],
        host=pg["host"],
        port=pg["port"],
        database=pg["database"],
    )
    return sa.create_engine(url, future=True)

DDL = [
    "CREATE SCHEMA IF NOT EXISTS raw;",
    """CREATE TABLE IF NOT EXISTS raw.customers(
        customer_id INT PRIMARY KEY,
        email TEXT NOT NULL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        signup_date DATE NOT NULL,
        state TEXT NOT NULL,
        segment TEXT NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS raw.products(
        sku TEXT PRIMARY KEY,
        category TEXT NOT NULL,
        brand TEXT NOT NULL,
        unit_cost NUMERIC(12,2) NOT NULL,
        unit_price NUMERIC(12,2) NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS raw.orders(
        order_id INT PRIMARY KEY,
        order_ts TIMESTAMP NOT NULL,
        customer_id INT NOT NULL,
        channel TEXT NOT NULL,
        payment_method TEXT NOT NULL,
        status TEXT NOT NULL,
        ship_state TEXT NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS raw.order_lines(
        order_line_id INT PRIMARY KEY,
        order_id INT NOT NULL,
        sku TEXT NOT NULL,
        quantity INT NOT NULL,
        unit_price NUMERIC(12,2) NOT NULL,
        unit_cost NUMERIC(12,2) NOT NULL,
        discount_rate NUMERIC(5,2) NOT NULL
    );"""
]

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    raw_dir = Path(cfg["paths"]["raw_dir"])
    pg = cfg["postgres"]
    eng = _engine(pg)

    with eng.begin() as conn:
        for stmt in DDL:
            conn.exec_driver_sql(stmt)

    # Load static tables
    for table, file in [("customers","customers.csv"), ("products","products.csv")]:
        df = pd.read_csv(raw_dir / file)
        df.to_sql(table, eng, schema="raw", if_exists="replace", index=False, method="multi")

    # Load monthly batches into raw.orders and raw.order_lines
    order_files = sorted(raw_dir.glob("orders_*.csv"))
    line_files = sorted(raw_dir.glob("order_lines_*.csv"))
    assert order_files, "No orders_*.csv found. Run src/generate_raw_data.py first."
    for f in order_files:
        df = pd.read_csv(f, parse_dates=["order_ts"])
        df.to_sql("orders", eng, schema="raw", if_exists="append", index=False, method="multi")
    for f in line_files:
        df = pd.read_csv(f)
        df.to_sql("order_lines", eng, schema="raw", if_exists="append", index=False, method="multi")

    print("Loaded raw tables into Postgres schema=raw.")

if __name__ == "__main__":
    main()
