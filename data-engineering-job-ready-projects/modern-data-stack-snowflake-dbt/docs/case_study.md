# Case Study: Modern Data Stack Analytics Engineering Project
**Snowflake + dbt | End-to-End Analytics Engineering Workflow**  
Author: Jamie Christian

---

## 1. Business Problem

A growing e-commerce marketplace lacked a reliable analytics foundation:
- Finance reported inconsistent revenue numbers
- Product teams could not trust funnel metrics
- Leadership lacked executive-level KPIs
- Analysts spent more time fixing data than analyzing it

The company needed a **scalable analytics engineering foundation** that could:
- Centralize raw data
- Apply consistent transformations
- Enforce data quality
- Deliver BI-ready datasets
- Support executive and operational decision-making

---

## 2. Objectives

- Build a modern data stack using **Snowflake + dbt**
- Implement layered modeling: **RAW → Staging → Intermediate → Marts**
- Enforce data quality with automated tests
- Create analytics-ready fact and dimension tables
- Support common stakeholder questions:
  - How is revenue trending?
  - Which channels drive the most conversions?
  - Who are our most valuable customers?
  - How do refunds impact net revenue?

---

## 3. Architecture Overview

**Pipeline Flow:**
```
Raw CSV Sources  
   ↓  
Snowflake RAW Schema  
   ↓  
dbt Staging Models (cleaning + typing)  
   ↓  
dbt Intermediate Models (business logic)  
   ↓  
Analytics Marts (Facts & Dimensions)  
   ↓  
BI Tools (Power BI / Tableau / Looker)
```

This structure mirrors real production analytics engineering systems.

---

## 4. Data Modeling Approach

### Staging Layer
Purpose: standardize raw data  
Examples:
- `stg_customers` → type casting, country normalization
- `stg_order_items` → derived `line_revenue`
- `stg_events` → null handling for optional IDs

### Intermediate Layer
Purpose: apply business logic  
Examples:
- `int_order_enriched` → gross revenue, net revenue, refunds
- `int_customer_ltv` → lifetime value, order frequency, AOV
- `int_event_funnel_daily` → sessions and funnel metrics

### Analytics Marts
Purpose: deliver BI-ready tables

**Fact Tables:**
- `fct_orders`
- `fct_order_items`
- `fct_events_daily`

**Dimension Tables:**
- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_date`

This star-schema structure supports fast querying and dashboard performance.

---

## 5. Data Quality & Governance

Implemented dbt tests to ensure trust:
- `not_null` and `unique` on primary keys
- Relationship tests between facts and dimensions
- Custom business-rule test:
  > Refunds should never exceed gross revenue per order

This ensures broken data is caught before reaching stakeholders.

---

## 6. Advanced Analytics Engineering Features

- **Snapshots (SCD2)**  
  Tracks historical changes in product pricing using `snp_products_scd2`.

- **Macros**  
  `safe_divide()` prevents divide-by-zero errors in metric logic.

- **Exposures**  
  Documents downstream dashboards as analytics contracts.

- **CI/CD Simulation**  
  GitHub Actions workflow validates dbt structure on each commit.

These elements demonstrate production-grade analytics engineering maturity.

---

## 7. Example Business Insights Enabled

Once models are built, stakeholders can immediately answer:

- Revenue growth by month and category
- Funnel conversion rate by traffic source
- Customer LTV and retention trends
- Refund impact on profitability
- Top customers and top-performing products

This transforms analytics from reactive reporting to proactive decision support.

---

## 8. Business Impact (Simulated)

If deployed in a real organization, this system would:
- Reduce analyst time spent cleaning data by **60–80%**
- Increase leadership trust in dashboards and KPIs
- Enable faster experimentation and performance tracking
- Provide a scalable foundation for future ML and advanced analytics

---

## 9. Skills Demonstrated

- Analytics Engineering (dbt)
- Dimensional Data Modeling
- Snowflake SQL Development
- Data Quality Testing & Governance
- CI/CD for Analytics Workflows
- Stakeholder-Oriented Data Design
- Metrics Engineering & KPI Design
- Portfolio-Ready Documentation

---

## 10. How to Talk About This in Interviews

> “I built a complete modern data stack project using Snowflake and dbt.  
> I designed layered models from raw ingestion through analytics marts, implemented automated data quality testing, created fact and dimension tables for BI, and added CI validation and documentation artifacts like exposures and architecture diagrams. This mirrors how analytics engineering teams operate in production.”

---

This case study is intentionally structured to resemble real-world analytics engineering work rather than a toy portfolio project.