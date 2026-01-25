from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import sqlalchemy as sa

TOP_CATEGORIES_MONTHLY_SQL = "\nWITH revenue AS (\n  SELECT\n    date_trunc('month', o.order_ts)::date AS month,\n    p.category,\n    SUM(ol.quantity * ol.unit_price * (1 - ol.discount_rate)) AS net_revenue,\n    SUM(ol.quantity * ol.unit_cost) AS cogs,\n    SUM(\n      CASE WHEN o.status='refunded'\n           THEN (ol.quantity * ol.unit_price * (1 - ol.discount_rate))\n           ELSE 0 END\n    ) AS refunds,\n    COUNT(DISTINCT o.order_id) AS orders\n  FROM raw.orders o\n  JOIN raw.order_lines ol ON ol.order_id = o.order_id\n  JOIN raw.products p ON p.sku = ol.sku\n  GROUP BY 1,2\n),\nranked AS (\n  SELECT\n    month,\n    category,\n    net_revenue,\n    cogs,\n    (net_revenue - cogs) AS gross_margin,\n    refunds,\n    (refunds / NULLIF(net_revenue + refunds, 0)) AS refund_rate,\n    orders,\n    ROW_NUMBER() OVER (PARTITION BY month ORDER BY net_revenue DESC) AS rn\n  FROM revenue\n)\nSELECT\n  month,\n  rn AS category_rank,\n  category,\n  ROUND(net_revenue::numeric, 2) AS net_revenue,\n  ROUND(cogs::numeric, 2) AS cogs,\n  ROUND(gross_margin::numeric, 2) AS gross_margin,\n  ROUND(refunds::numeric, 2) AS refunds,\n  ROUND(refund_rate::numeric, 4) AS refund_rate,\n  orders\nFROM ranked\nWHERE rn <= 5\nORDER BY month, rn;\n"

KPI_SUMMARY_SQL = "\nWITH monthly AS (\n  SELECT\n    date_trunc('month', o.order_ts)::date AS month,\n    COUNT(DISTINCT o.order_id) AS orders,\n    COUNT(DISTINCT o.customer_id) AS active_customers,\n    SUM(ol.quantity * ol.unit_price * (1 - ol.discount_rate)) AS net_revenue,\n    SUM(ol.quantity * ol.unit_cost) AS cogs,\n    SUM(\n      CASE WHEN o.status='refunded'\n           THEN (ol.quantity * ol.unit_price * (1 - ol.discount_rate))\n           ELSE 0 END\n    ) AS refunds\n  FROM raw.orders o\n  JOIN raw.order_lines ol ON ol.order_id = o.order_id\n  GROUP BY 1\n)\nSELECT\n  month,\n  orders,\n  active_customers,\n  ROUND(net_revenue::numeric, 2) AS net_revenue,\n  ROUND(cogs::numeric, 2) AS cogs,\n  ROUND((net_revenue - cogs)::numeric, 2) AS gross_margin,\n  ROUND(((net_revenue - cogs) / NULLIF(net_revenue, 0))::numeric, 4) AS gross_margin_pct,\n  ROUND(refunds::numeric, 2) AS refunds,\n  ROUND((refunds / NULLIF(net_revenue + refunds, 0))::numeric, 4) AS refund_rate,\n  ROUND((net_revenue / NULLIF(orders, 0))::numeric, 2) AS aov\nFROM monthly\nORDER BY month;\n"

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

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    outdir = Path(cfg["paths"]["outputs_dir"])
    outdir.mkdir(parents=True, exist_ok=True)
    eng = _engine(cfg["postgres"])

    df_top = pd.read_sql(TOP_CATEGORIES_MONTHLY_SQL, eng)
    df_sum = pd.read_sql(KPI_SUMMARY_SQL, eng)

    df_top.to_csv(outdir / "top_categories_monthly.csv", index=False)
    df_sum.to_csv(outdir / "kpi_summary.csv", index=False)
    print(f"Wrote outputs to {outdir.resolve()}")

if __name__ == "__main__":
    main()
