
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from jinja2 import Template
from scipy.stats import ks_2samp, chi2_contingency
from sklearn.ensemble import IsolationForest

from .logutil import get_logger
from .alerting import write_alerts, post_slack

log = get_logger("p3.monitor")

HTML_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Data Reliability Report — {{ report_date }}</title>
  <style>
    body { font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
    h1 { margin-bottom: 0; }
    .muted { color: #555; margin-top: 4px; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 16px; margin: 14px 0; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border-bottom: 1px solid #eee; padding: 8px; text-align: left; vertical-align: top; }
    .ok { color: #0a7; font-weight: 600; }
    .bad { color: #c22; font-weight: 700; }
    .warn { color: #b80; font-weight: 700; }
    code { background: #f6f6f6; padding: 2px 6px; border-radius: 6px; }
  </style>
</head>
<body>
  <h1>Data Reliability Report</h1>
  <div class="muted">Dataset: <code>{{ dataset }}</code> — Report date: <code>{{ report_date }}</code></div>

  <div class="card">
    <h2>Overall health</h2>
    <p><strong>Health score:</strong> {{ health_score }} / 100
       {% if health_score >= 90 %}<span class="ok">OK</span>
       {% elif health_score >= 75 %}<span class="warn">WATCH</span>
       {% else %}<span class="bad">FAIL</span>{% endif %}
    </p>
    <p><strong>Rows:</strong> {{ row_count }} | <strong>Columns:</strong> {{ col_count }}</p>
  </div>

  <div class="card">
    <h2>Rule checks</h2>
    <table>
      <thead><tr><th>Rule</th><th>Status</th><th>Details</th></tr></thead>
      <tbody>
        {% for r in rules %}
        <tr>
          <td>{{ r.name }}</td>
          <td>{% if r.passed %}<span class="ok">PASS</span>{% else %}<span class="bad">FAIL</span>{% endif %}</td>
          <td>{{ r.details }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>Drift tests</h2>
    <table>
      <thead><tr><th>Column</th><th>Type</th><th>p-value</th><th>Drift?</th><th>Notes</th></tr></thead>
      <tbody>
        {% for d in drift %}
        <tr>
          <td>{{ d.column }}</td>
          <td>{{ d.type }}</td>
          <td>{{ "%.4g"|format(d.p_value) }}</td>
          <td>{% if d.drifted %}<span class="warn">YES</span>{% else %}<span class="ok">NO</span>{% endif %}</td>
          <td>{{ d.notes }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>Metric anomalies</h2>
    <table>
      <thead><tr><th>Metric</th><th>Value</th><th>Anomaly?</th><th>Context</th></tr></thead>
      <tbody>
        {% for m in anomalies %}
        <tr>
          <td>{{ m.metric }}</td>
          <td>{{ m.value }}</td>
          <td>{% if m.is_anomaly %}<span class="warn">YES</span>{% else %}<span class="ok">NO</span>{% endif %}</td>
          <td>{{ m.context }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>Recommended actions</h2>
    <ul>
      {% for a in actions %}
      <li>{{ a }}</li>
      {% endfor %}
    </ul>
  </div>

</body>
</html>
"""

@dataclass
class RuleResult:
    name: str
    passed: bool
    details: str

def load_expectations(path: Path) -> dict:
    return json.loads(path.read_text())

def validate_schema(df: pd.DataFrame, expected_schema: dict) -> RuleResult:
    missing = [c for c in expected_schema.keys() if c not in df.columns]
    extra = [c for c in df.columns if c not in expected_schema.keys()]
    passed = (len(missing) == 0)
    details = f"missing={missing} extra={extra}"
    return RuleResult("schema_columns_present", passed, details)

def check_unique(df: pd.DataFrame, col: str) -> RuleResult:
    dup = int(df[col].duplicated().sum())
    return RuleResult(f"unique:{col}", dup == 0, f"duplicates={dup}")

def check_non_null(df: pd.DataFrame, cols: list[str]) -> RuleResult:
    nulls = {c: int(df[c].isna().sum()) for c in cols}
    passed = all(v == 0 for v in nulls.values())
    return RuleResult(f"non_null:{','.join(cols)}", passed, f"nulls={nulls}")

def check_range(df: pd.DataFrame, col: str, min_v: float, max_v: float) -> RuleResult:
    bad = int(((df[col] < min_v) | (df[col] > max_v) | (df[col].isna())).sum())
    passed = bad == 0
    return RuleResult(f"range:{col}", passed, f"out_of_range_or_null={bad}")

def check_rate_max(df: pd.DataFrame, col: str, max_rate: float) -> RuleResult:
    rate = float(df[col].mean())
    return RuleResult(f"rate_max:{col}", rate <= max_rate, f"rate={rate:.4f} max={max_rate:.4f}")

def drift_numeric(baseline: pd.Series, current: pd.Series, alpha: float) -> dict:
    base = baseline.dropna().astype(float)
    cur = current.dropna().astype(float)
    if len(base) < 20 or len(cur) < 20:
        return {"p_value": 1.0, "drifted": False, "notes":"insufficient sample"}
    stat = ks_2samp(base, cur)
    return {"p_value": float(stat.pvalue), "drifted": float(stat.pvalue) < alpha, "notes": f"KS={stat.statistic:.4f}"}

def drift_categorical(baseline: pd.Series, current: pd.Series, alpha: float) -> dict:
    base = baseline.fillna("NA").astype(str)
    cur = current.fillna("NA").astype(str)
    cats = sorted(set(base.unique()).union(set(cur.unique())))
    base_counts = np.array([(base == c).sum() for c in cats])
    cur_counts = np.array([(cur == c).sum() for c in cats])
    table = np.vstack([base_counts, cur_counts])
    try:
        chi2, p, dof, _ = chi2_contingency(table)
        return {"p_value": float(p), "drifted": float(p) < alpha, "notes": f"chi2={chi2:.2f} dof={dof}"}
    except Exception as e:
        return {"p_value": 1.0, "drifted": False, "notes": f"chi2_failed:{e}"}

def compute_metrics(df: pd.DataFrame) -> dict:
    return {
        "rows": int(len(df)),
        "null_rate_discount": float(df["discount"].isna().mean()),
        "avg_order_value": float(df["order_value"].dropna().mean()),
        "p95_order_value": float(np.nanpercentile(df["order_value"].values, 95)),
        "return_rate": float(df["is_return"].mean()),
        "mobile_share": float((df["channel"] == "mobile").mean()),
        "items_mean": float(df["items"].replace([np.inf,-np.inf], np.nan).dropna().mean()),
    }

def anomaly_detection(metric_history: pd.DataFrame, metric_today: dict) -> list[dict]:
    """
    Fits IsolationForest on metric history (last N days) and scores today's metrics.
    """
    hist = metric_history.copy()
    cols = [c for c in hist.columns if c != "date"]
    X = hist[cols].values
    model = IsolationForest(n_estimators=300, contamination=0.15, random_state=42)
    model.fit(X)

    today_vec = np.array([[metric_today[c] for c in cols]])
    pred = model.predict(today_vec)[0]  # -1 anomaly
    score = model.decision_function(today_vec)[0]

    results = []
    for c in cols:
        results.append({
            "metric": c,
            "value": round(float(metric_today[c]), 6),
            "is_anomaly": bool(pred == -1),
            "context": f"model_score={score:.4f} (shared across metrics for this run)"
        })
    return results

def health_score(rule_results: list[RuleResult], drift_results: list[dict], anomalies: list[dict]) -> int:
    score = 100
    fails = sum(1 for r in rule_results if not r.passed)
    score -= fails * 18

    drifted = sum(1 for d in drift_results if d["drifted"])
    score -= min(25, drifted * 6)

    is_anom = any(a["is_anomaly"] for a in anomalies)
    if is_anom:
        score -= 12

    return max(0, min(100, int(score)))

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", type=str, default="run", choices=["run","baseline"], help="run=compare current vs baseline; baseline=write baseline summary + self-check")
    ap.add_argument("--day_offset", type=int, default=0, help="0 = most recent day file, 7 = a week ago, etc.")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    exp = load_expectations(root / "configs" / "expectations.json")

    # choose date file
    report_day = date.today() - timedelta(days=args.day_offset)
    current_path = root / "data" / "raw" / f"orders_{report_day.isoformat()}.csv"
    baseline_path = root / "data" / "baseline" / "baseline_orders.csv"

    if args.mode == "baseline":
        current_path = baseline_path
        report_day = date.today()  # report timestamp


    if not current_path.exists() or not baseline_path.exists():
        raise FileNotFoundError("Missing data files. Run: python -m src.generate_data")

    current = pd.read_csv(current_path)
    baseline = pd.read_csv(baseline_path)

    rule_results: list[RuleResult] = []
    rule_results.append(validate_schema(current, exp["schema"]))

    for rule in exp["rules"]:
        t = rule["type"]
        if t == "unique":
            rule_results.append(check_unique(current, rule["column"]))
        elif t == "non_null":
            rule_results.append(check_non_null(current, rule["columns"]))
        elif t == "range":
            rule_results.append(check_range(current, rule["column"], float(rule["min"]), float(rule["max"])))
        elif t == "rate_max":
            rule_results.append(check_rate_max(current, rule["column"], float(rule["max"])))
        else:
            raise ValueError(f"Unsupported rule type: {t}")

    # drift tests
    alpha = float(exp["drift"]["alpha"])
    drift_results = []

    for col in exp["drift"]["numeric_columns"]:
        res = drift_numeric(baseline[col], current[col], alpha)
        drift_results.append({"column": col, "type": "numeric", **res})

    for col in exp["drift"]["categorical_columns"]:
        res = drift_categorical(baseline[col], current[col], alpha)
        drift_results.append({"column": col, "type": "categorical", **res})

    # metric history for anomaly model: load the last 14 days if present
    history = []
    for i in range(1, 15):
        d = report_day - timedelta(days=i)
        p = root / "data" / "raw" / f"orders_{d.isoformat()}.csv"
        if p.exists():
            df = pd.read_csv(p)
            m = compute_metrics(df)
            m["date"] = d.isoformat()
            history.append(m)
    metric_today = compute_metrics(current)

    if len(history) >= 7:
        hist_df = pd.DataFrame(history).sort_values("date")
        anomalies = anomaly_detection(hist_df, metric_today)
    else:
        anomalies = [{"metric": k, "value": round(float(v), 6), "is_anomaly": False, "context":"insufficient history"} for k, v in metric_today.items() if k != "rows"]

    hs = health_score(rule_results, drift_results, anomalies)

    actions = []
    if any(not r.passed for r in rule_results):
        actions.append("Fix failing rule checks first (schema/range/null/uniqueness) — these usually indicate pipeline defects.")
    if any(d["drifted"] for d in drift_results):
        actions.append("Investigate drifted columns: check upstream sources, product changes, pricing/promo flags, or traffic/channel mix shifts.")
    if any(a["is_anomaly"] for a in anomalies):
        actions.append("Metric anomaly detected: compare against recent days and confirm no ingestion gaps or partial loads.")
    if not actions:
        actions.append("No action required. Continue monitoring daily and pin this report to the pipeline run artifacts.")

    report = {
        "dataset": exp["dataset"],
        "report_date": report_day.isoformat(),
        "row_count": int(len(current)),
        "col_count": int(current.shape[1]),
        "health_score": hs,
        "rules": [r.__dict__ for r in rule_results],
        "drift": drift_results,
        "anomalies": anomalies,
        "actions": actions,
    }

    reports_dir = root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / f"report_{report_day.isoformat()}.json"
    json_path.write_text(json.dumps(report, indent=2))

    html = Template(HTML_TEMPLATE).render(**report)
    html_path = reports_dir / f"report_{report_day.isoformat()}.html"
    html_path.write_text(html, encoding="utf-8")

    log.info("Wrote %s and %s", html_path, json_path)
    log.info("Health score = %s/100", hs)

if __name__ == "__main__":
    main()
