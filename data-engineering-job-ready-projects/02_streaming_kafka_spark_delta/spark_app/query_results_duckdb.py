import duckdb

con = duckdb.connect()
# DuckDB can read delta as parquet by pointing at files; for demo we read parquet files under delta folder (Spark writes parquet + _delta_log).
# We read only parquet data files.
df = con.execute("SELECT * FROM read_parquet('lakehouse/delta/clickstream_agg/*.parquet') LIMIT 20").df()
print(df)
