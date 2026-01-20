# Modern Data Stack Analytics Engineering Project (Snowflake + dbt)

A complete, job-ready analytics engineering project that mirrors a real production workflow:
- **Raw CSV ingestion into Snowflake (RAW schema)**
- **dbt transformations (staging → intermediate → marts)**
- **Data quality tests + documentation + snapshot example**
- **Star-schema style facts/dimensions for BI (Power BI/Tableau/Looker)**

This repo is designed to be copied into GitHub as a portfolio project.

---

## What’s Included

### Data (fully generated, no placeholders)
Located in: `data/raw/`
- `customers.csv` (8,000 rows)
- `products.csv` (450 rows)
- `sellers.csv` (1,200 rows)
- `orders.csv` (20,000 rows)
- `order_items.csv` (35,545 rows)
- `refunds.csv` (2,469 rows)
- `events.csv` (200,000 rows)

### Snowflake SQL
- `snowflake/sql/00_setup_and_load_raw.sql`  
  Creates warehouse, database, schemas, stage, file format, raw tables, and COPY INTO commands.

### dbt Project
Located in: `dbt/`
- **Staging**: clean + type-cast raw tables
- **Intermediate**: business logic & rollups (`int_order_enriched`, `int_customer_ltv`, `int_event_funnel_daily`)
- **Marts**: BI-ready facts/dims (`fct_orders`, `fct_order_items`, `fct_events_daily`, `dim_*`)  
- **Tests**: unique/not_null/relationships + custom logic tests
- **Snapshot**: `snp_products_scd2` shows SCD2 tracking on products

---

## Quickstart (End-to-End)

### 1) Load raw data into Snowflake
1. Run: `snowflake/sql/00_setup_and_load_raw.sql` in Snowsight
2. Upload the CSVs to the internal stage:
   - Snowsight → **Data** → Databases → `MODERN_DATA_STACK` → Schema `RAW` → Stage `RAW_STAGE` → **+ Files**
3. Run the COPY commands included in the SQL script.
4. Confirm row counts match the README above.

### 2) Run dbt against Snowflake
From the `dbt/` directory:

```bash
dbt deps
dbt debug
dbt build
dbt docs generate
dbt docs serve
```

> `profiles.yml` uses environment variables, so credentials aren’t stored in the repo.
> Export env vars in your terminal, e.g. `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`.

### 3) Tables you’ll have for BI
Schema: `MODERN_DATA_STACK.ANALYTICS`
- `DIM_DATE`, `DIM_CUSTOMER`, `DIM_PRODUCT`, `DIM_SELLER`
- `FCT_ORDERS`, `FCT_ORDER_ITEMS`, `FCT_EVENTS_DAILY`
- `VW_KPI_DAILY`

---

## Stakeholder Questions (Ready to Demo)

Use `dbt/analysis/example_stakeholder_queries.sql` as a live demo script in interviews:
- Revenue and gross profit by category/month
- Conversion rate by traffic source
- Top customers by lifetime revenue (LTV)

---

## Why This Is “10/10” for Recruiters
- Clear layered modeling (raw → staging → intermediate → marts)
- Test suite with relationships & business rules
- SCD2 snapshot pattern included
- BI-ready star schema for dashboards
- Data volumes large enough to feel realistic but still fast to run locally

---

## Suggested Portfolio Story (Use in README / LinkedIn)
**Modern Data Stack (Snowflake + dbt):** Built an end-to-end analytics engineering workflow with raw ingestion, typed staging models, business logic rollups, BI-ready marts, automated tests, and an SCD2 snapshot pattern—delivering curated facts/dimensions that support finance + product KPI reporting (revenue, profit, refunds, funnel conversion, and customer LTV).


---

## 11/10 Enhancements Added

This project now goes beyond typical portfolio examples by including:

### CI/CD Simulation (GitHub Actions)
`.github/workflows/dbt_ci.yml`
- Demonstrates automated dbt validation on every push / PR.
- Mirrors real analytics engineering team workflows.

### Architecture Diagram
`docs/architecture.svg`
- Visual system design: sources → Snowflake RAW → dbt layers → BI.
- Recruiters love seeing system-level thinking, not just SQL.

### dbt Exposures
`dbt/models/marts/exposures.yml`
- Shows how downstream dashboards depend on models.
- Reflects true production dbt usage and analytics contracts.

These additions push the project into **senior-quality structure**, even though it's a portfolio.



---

## Case Study
A full professional write-up is included here:
📄 `docs/case_study.md`

This document walks through:
- Business problem
- Architecture decisions
- Data modeling approach
- Testing & governance
- Stakeholder impact
- Interview-ready talking points

This makes the project usable not just as code, but as a **complete portfolio artifact**.

