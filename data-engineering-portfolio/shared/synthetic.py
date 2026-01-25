import pandas as pd
import numpy as np

def make_retail_orders(n_customers: int = 200, n_orders: int = 4000, start: str = "2025-01-01", end: str = "2025-12-31"):
    rng = np.random.default_rng(42)
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)
    dates = start_dt + pd.to_timedelta(rng.integers(0, (end_dt-start_dt).days+1, size=n_orders), unit="D")

    customers = pd.DataFrame({
        "customer_id": np.arange(1, n_customers+1),
        "email": [f"cust{cid}@example.com" for cid in range(1, n_customers+1)],
        "first_name": rng.choice(["Ava","Mia","Noah","Liam","Sophia","Olivia","James","Ethan","Emma","Amir"], size=n_customers),
        "last_name": rng.choice(["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis"], size=n_customers),
        "signup_date": start_dt + pd.to_timedelta(rng.integers(0, 365, size=n_customers), unit="D"),
        "state": rng.choice(["CA","NY","TX","FL","NC","WA","IL","GA"], size=n_customers),
        "segment": rng.choice(["SMB","MidMarket","Enterprise"], p=[0.7,0.25,0.05], size=n_customers)
    })

    sku = pd.DataFrame({
        "sku": [f"SKU-{i:04d}" for i in range(1, 201)],
        "category": rng.choice(["Electronics","Home","Beauty","Sports","Grocery","Books"], size=200),
        "brand": rng.choice(["Acme","Nova","Zenith","Pulse","Orchid","Nimbus"], size=200),
        "unit_cost": np.round(rng.uniform(2, 120, size=200), 2),
        "unit_price": np.round(rng.uniform(5, 250, size=200), 2),
    })

    orders = pd.DataFrame({
        "order_id": np.arange(1, n_orders+1),
        "order_ts": dates + pd.to_timedelta(rng.integers(0, 24*3600, size=n_orders), unit="s"),
        "customer_id": rng.integers(1, n_customers+1, size=n_orders),
        "channel": rng.choice(["web","mobile","partner"], p=[0.55,0.35,0.10], size=n_orders),
        "payment_method": rng.choice(["card","paypal","apple_pay","klarna"], p=[0.7,0.15,0.1,0.05], size=n_orders),
        "status": rng.choice(["paid","refunded","chargeback"], p=[0.955,0.035,0.01], size=n_orders),
        "ship_state": rng.choice(["CA","NY","TX","FL","NC","WA","IL","GA"], size=n_orders)
    })

    line_ct = rng.integers(1, 5, size=n_orders)
    lines = []
    line_id = 1
    for oid, ct in zip(orders["order_id"].values, line_ct):
        picks = rng.choice(sku["sku"].values, size=ct, replace=False)
        qty = rng.integers(1, 4, size=ct)
        for s, q in zip(picks, qty):
            p = float(sku.loc[sku["sku"]==s, "unit_price"].iloc[0])
            c = float(sku.loc[sku["sku"]==s, "unit_cost"].iloc[0])
            discount = float(np.round(rng.choice([0,0,0,0.05,0.10,0.15], p=[0.6,0.1,0.1,0.1,0.07,0.03]), 2))
            lines.append([line_id, oid, s, int(q), p, c, discount])
            line_id += 1

    order_lines = pd.DataFrame(lines, columns=["order_line_id","order_id","sku","quantity","unit_price","unit_cost","discount_rate"])
    return customers, sku, orders, order_lines
