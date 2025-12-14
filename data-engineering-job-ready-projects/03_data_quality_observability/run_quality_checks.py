from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
import great_expectations as gx

BASE = Path(__file__).resolve().parents[0]
DATA = BASE / "data" / "bronze" / "orders_bronze.csv"
REPORTS = BASE / "reports"
REPORTS.mkdir(exist_ok=True)

def build_context() -> gx.DataContext:
    context_root = BASE / "gx"
    context_root.mkdir(exist_ok=True)
    return gx.get_context(context_root_dir=str(context_root))

def main() -> None:
    context = build_context()

    df = pd.read_csv(DATA)
    # Create a pandas datasource + asset
    datasource_name = "pandas_orders"
    datasource = context.sources.add_pandas(name=datasource_name)
    asset = datasource.add_dataframe_asset(name="orders_bronze")

    batch_request = asset.build_batch_request(dataframe=df)
    suite_name = "orders_bronze_suite"

    try:
        suite = context.add_expectation_suite(suite_name)
    except Exception:
        suite = context.get_expectation_suite(suite_name)

    # Define expectations (quality rules)
    validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

    validator.expect_column_values_to_be_unique("order_id")
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_match_regex("order_date", r"^\d{4}-\d{2}-\d{2}$")
    validator.expect_column_values_to_be_between("amount_usd", min_value=0, max_value=1000)
    validator.expect_column_values_to_be_in_set("status", ["paid", "refunded", "failed"])

    validator.save_expectation_suite(discard_failed_expectations=False)

    # Run checkpoint
    checkpoint_name = "orders_bronze_checkpoint"
    checkpoint = gx.checkpoint.SimpleCheckpoint(
        name=checkpoint_name,
        data_context=context,
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }
        ],
    )

    result = checkpoint.run()
    # Save a compact JSON report
    out_json = REPORTS / "validation_result.json"
    out_json.write_text(json.dumps(result.to_json_dict(), indent=2), encoding="utf-8")

    # Summarize failures
    success = result.success
    failed = []
    for run_result in result.run_results.values():
        v = run_result["validation_result"]
        for r in v["results"]:
            if not r["success"]:
                failed.append({
                    "expectation_type": r["expectation_config"]["expectation_type"],
                    "column": r["expectation_config"]["kwargs"].get("column"),
                    "result": r["result"],
                })

    summary = {
        "dataset": str(DATA),
        "success": success,
        "failed_expectations": failed,
        "row_count": len(df),
        "failure_count": len(failed),
    }
    (REPORTS / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Great Expectations run complete.")
    print(json.dumps(summary, indent=2))

    if not success:
        raise SystemExit("Data quality checks failed — see reports/ for details.")

if __name__ == "__main__":
    main()
