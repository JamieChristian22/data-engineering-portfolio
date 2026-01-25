from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from shared.utils import ensure_dir
from shared.synthetic import make_retail_orders

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    raw_dir = ensure_dir(Path(cfg["paths"]["raw_dir"]))
    # Generate one year of synthetic "extracts" (customers, products, orders, order_lines)
    customers, products, orders, order_lines = make_retail_orders()

    # Split orders into monthly extracts to simulate batch ingestion
    orders["order_date"] = pd.to_datetime(orders["order_ts"]).dt.date
    orders["order_month"] = pd.to_datetime(orders["order_ts"]).dt.to_period("M").astype(str)

    for month, chunk in orders.groupby("order_month"):
        out = raw_dir / f"orders_{month}.csv"
        chunk.drop(columns=["order_month"]).to_csv(out, index=False)

        # matching order lines
        oids = set(chunk["order_id"].tolist())
        ol_chunk = order_lines[order_lines["order_id"].isin(oids)]
        ol_chunk.to_csv(raw_dir / f"order_lines_{month}.csv", index=False)

    customers.to_csv(raw_dir / "customers.csv", index=False)
    products.to_csv(raw_dir / "products.csv", index=False)

    print(f"Wrote raw extracts to: {raw_dir.resolve()}")

if __name__ == "__main__":
    main()
