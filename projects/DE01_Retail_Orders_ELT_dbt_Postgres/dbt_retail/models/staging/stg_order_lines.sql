with src as (
  select * from raw.order_lines
),
typed as (
  select
    order_line_id::int as order_line_id,
    order_id::int as order_id,
    sku,
    quantity::int as quantity,
    unit_price::numeric(12,2) as unit_price,
    unit_cost::numeric(12,2) as unit_cost,
    discount_rate::numeric(5,2) as discount_rate
  from src
)
select * from typed
