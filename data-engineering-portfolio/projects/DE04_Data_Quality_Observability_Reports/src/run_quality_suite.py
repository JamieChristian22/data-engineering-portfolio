from __future__ import annotations
import json, math
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from shared.utils import ensure_dir, utcnow_iso

def pct(x: float) -> str:
    return f"{x*100:.2f}%"

def html_report(report: dict) -> str:
    # Simple, self-contained HTML (no placeholders)
    checks_html = ""
    for c in report["checks"]:
        status = "✅ PASS" if c["pass"] else "❌ FAIL"
        checks_html += f"<tr><td>{c['name']}</td><td>{status}</td><td><pre>{json.dumps(c['details'], indent=2)}</pre></td></tr>"
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Data Quality Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    td, th {{ border: 1px solid #ddd; padding: 10px; vertical-align: top; }}
    th {{ background: #f4f4f4; text-align: left; }}
    .pass {{ color: #0a7; font-weight: 700; }}
    .fail {{ color: #c00; font-weight: 700; }}
  </style>
</head>
<body>
  <h1>Data Quality Report</h1>
  <p><b>Run at:</b> {report['run_at']}</p>
  <p><b>Dataset:</b> {report['dataset']}</p>
  <h2>Summary</h2>
  <ul>
    <li><b>Rows:</b> {report['metrics']['rows']}</li>
    <li><b>Refund rate:</b> {pct(report['metrics']['refund_rate'])}</li>
    <li><b>Max null rate:</b> {pct(report['metrics']['max_null_rate'])}</li>
    <li><b>Freshness:</b> {report['metrics']['freshness_hours']:.2f} hours</li>
  </ul>
  <h2>Checks</h2>
  <table>
    <tr><th>Check</th><th>Status</th><th>Details</th></tr>
    {checks_html}
  </table>
</body>
</html>"""


def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    data_dir = Path(cfg["data_dir"])
    out_dir = ensure_dir(Path(cfg["outputs_dir"]))

    ds = data_dir / "daily_orders_fact.csv"
    df = pd.read_csv(ds)

    # Metrics
    rows = len(df)
    null_rates = df.isna().mean(numeric_only=False).to_dict()
    max_null_rate = max(null_rates.values()) if null_rates else 0.0
    refund_rate = float(df["is_refund"].mean())
    # Freshness: compare newest loaded_at to now
    newest_loaded = pd.to_datetime(df["loaded_at"]).max()
    now = datetime.now(timezone.utc)
    freshness_hours = float((now - newest_loaded.to_pydatetime()).total_seconds() / 3600)

    checks = []

    # Schema check
    expected_cols = ["event_date","order_id","customer_id","amount_usd","is_refund","loaded_at"]
    checks.append({
        "name": "schema_expected_columns",
        "pass": list(df.columns) == expected_cols,
        "details": {"expected": expected_cols, "actual": list(df.columns)}
    })

    # Uniqueness
    dup = int(df["order_id"].duplicated().sum())
    checks.append({
        "name": "order_id_unique",
        "pass": dup == 0,
        "details": {"duplicate_order_ids": dup}
    })

    # Null rate threshold
    checks.append({
        "name": "max_null_rate_under_threshold",
        "pass": max_null_rate <= cfg["anomaly"]["max_null_rate_any_column"],
        "details": {"max_null_rate": max_null_rate, "threshold": cfg["anomaly"]["max_null_rate_any_column"], "null_rates": null_rates}
    })

    # Volume range
    checks.append({
        "name": "row_count_in_range",
        "pass": cfg["anomaly"]["min_rows"] <= rows <= cfg["anomaly"]["max_rows"] * 14,  # 14 days dataset
        "details": {"rows": rows, "expected_min": cfg["anomaly"]["min_rows"], "expected_max": cfg["anomaly"]["max_rows"]*14}
    })

    # Refund rate
    checks.append({
        "name": "refund_rate_under_threshold",
        "pass": refund_rate <= cfg["anomaly"]["max_refund_rate"],
        "details": {"refund_rate": refund_rate, "threshold": cfg["anomaly"]["max_refund_rate"]}
    })

    # Freshness SLA
    checks.append({
        "name": "freshness_within_sla_hours",
        "pass": freshness_hours <= cfg["sla_hours"],
        "details": {"freshness_hours": freshness_hours, "sla_hours": cfg["sla_hours"], "newest_loaded_at": str(newest_loaded)}
    })

    report = {
        "run_at": utcnow_iso(),
        "dataset": str(ds),
        "metrics": {
            "rows": rows,
            "refund_rate": refund_rate,
            "max_null_rate": max_null_rate,
            "freshness_hours": freshness_hours
        },
        "checks": checks,
        "overall_pass": all(c["pass"] for c in checks)
    }

    (out_dir / "quality_report.json").write_text(json.dumps(report, indent=2))
    (out_dir / "quality_report.html").write_text(html_report(report), encoding="utf-8")
    print(json.dumps({"overall_pass": report["overall_pass"], "failed": [c["name"] for c in checks if not c["pass"]]}, indent=2))

if __name__ == "__main__":
    main()
