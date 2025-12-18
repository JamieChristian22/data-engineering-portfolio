from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DATA_DIR = "/opt/airflow/data/raw"

def _copy_sql(table: str, csv_name: str) -> str:
    path = os.path.join(DATA_DIR, csv_name)
    return f"""
    TRUNCATE TABLE raw.{table};
    COPY raw.{table}
    FROM '{path}'
    WITH (FORMAT csv, HEADER true);
    """

with DAG(
    dag_id="batch_etl_raw_to_mart",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["portfolio", "batch", "dbt", "postgres"],
) as dag:

    load_customers = PostgresOperator(
        task_id="load_customers_csv_to_raw",
        postgres_conn_id="postgres_default",
        sql=_copy_sql("customers", "customers.csv"),
    )

    load_orders = PostgresOperator(
        task_id="load_orders_csv_to_raw",
        postgres_conn_id="postgres_default",
        sql=_copy_sql("orders", "orders.csv"),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dags/../../transform/dbt && dbt run --project-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dags/../../transform/dbt && dbt test --project-dir .",
    )

    load_customers >> load_orders >> dbt_run >> dbt_test
