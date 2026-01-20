with orders as (
  select * from {{ ref('int_order_enriched') }}
),
cust as (
  select * from {{ ref('dim_customer') }}
)
select
  o.order_id,
  o.customer_id,
  o.order_ts,
  o.order_date,
  o.order_month,
  o.channel,
  o.traffic_source,
  o.campaign,
  o.payment_method,
  o.shipping_method,
  o.order_status,
  o.seller_id,
  o.total_units,
  o.distinct_products,
  o.gross_revenue,
  o.refund_amount,
  o.net_revenue,
  c.country_code,
  c.region,
  c.is_b2b,
  c.plan_tier
from orders o
join cust c
  on o.customer_id = c.customer_id
