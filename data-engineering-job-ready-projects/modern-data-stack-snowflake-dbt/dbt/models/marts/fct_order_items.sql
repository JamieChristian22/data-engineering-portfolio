with items as (
  select * from {{ ref('stg_order_items') }}
),
orders as (
  select order_id, customer_id, order_ts, order_status, seller_id from {{ ref('stg_orders') }}
),
prod as (
  select product_id, category, brand, unit_cost from {{ ref('dim_product') }}
)
select
  i.order_item_id,
  i.order_id,
  o.customer_id,
  o.order_ts,
  date_trunc('day', o.order_ts) as order_date,
  o.order_status,
  o.seller_id,
  i.product_id,
  p.category,
  p.brand,
  i.quantity,
  i.list_price,
  i.discount_pct,
  i.unit_price,
  i.line_revenue,
  cast(p.unit_cost * i.quantity as number(12,2)) as line_cost,
  cast(i.line_revenue - (p.unit_cost * i.quantity) as number(12,2)) as line_gross_profit
from items i
join orders o
  on i.order_id = o.order_id
join prod p
  on i.product_id = p.product_id
