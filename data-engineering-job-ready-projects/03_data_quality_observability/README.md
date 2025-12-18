# 03 — Data Quality & Observability with Great Expectations

## What this project demonstrates
- Defining **data quality rules** (uniqueness, non‑null, regex, ranges, allowed values).
- Running checks as part of a pipeline step (fails the run if quality breaks).
- Generating artifacts: JSON validation results + summary report.

## Run locally
```bash
make venv
make install
make run
```

Outputs:
- `reports/validation_result.json` (full GE results)
- `reports/summary.json` (human‑friendly summary)

## Quality rules
- `order_id` unique
- `customer_id` not null
- `order_date` is ISO `YYYY-MM-DD`
- `amount_usd` between 0 and 1000
- `status` in {paid, refunded, failed}

## What to screenshot for proof
- Terminal output showing the summary JSON
- The `reports/summary.json` file contents
