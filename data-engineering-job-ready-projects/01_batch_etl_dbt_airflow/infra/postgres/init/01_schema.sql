CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.customers (
  customer_id INT PRIMARY KEY,
  created_date DATE,
  state TEXT,
  city TEXT,
  email_domain TEXT
);

CREATE TABLE IF NOT EXISTS raw.orders (
  order_id INT PRIMARY KEY,
  customer_id INT REFERENCES raw.customers(customer_id),
  order_date DATE,
  channel TEXT,
  device TEXT,
  amount_usd NUMERIC(10,2),
  status TEXT
);

-- Load CSVs mounted by Airflow/dbt container to /opt/airflow/data/raw
-- Note: COPY runs inside Postgres; we use a small hack: Postgres can read from /docker-entrypoint-initdb.d only,
-- so we ship a loader that re-loads when containers start via Airflow DAG too.
