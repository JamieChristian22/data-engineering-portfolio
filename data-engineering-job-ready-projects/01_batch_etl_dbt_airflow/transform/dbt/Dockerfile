FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-postgres==1.8.2

WORKDIR /usr/app/dbt
CMD ["bash", "-lc", "dbt --version && tail -f /dev/null"]
