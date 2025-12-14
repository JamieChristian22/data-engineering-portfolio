# Lakehouse Layout

This project uses a classic lakehouse directory structure:

- `lakehouse/bronze/` — raw ingested parquet
- `lakehouse/silver/` — cleaned & conformed parquet
- `lakehouse/gold/` — aggregated marts parquet

This mirrors an AWS S3 lake + Glue/Athena catalogs conceptually, but runs locally with DuckDB for portability.
