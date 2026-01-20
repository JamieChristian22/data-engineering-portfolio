with orders as (
  select * from {{ ref('int_order_enriched') }}
),
cust as (
  select * from {{ ref('stg_customers') }}
),
agg as (
  select
    customer_id,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    count(*) as order_count,
    sum(net_revenue) as lifetime_net_revenue,
    avg(net_revenue) as avg_order_value
  from orders
  where order_status = 'completed'
  group by 1
)
select
  c.customer_id,
  c.signup_date,
  c.country_code,
  c.region,
  c.is_b2b,
  c.plan_tier,
  a.first_order_date,
  a.last_order_date,
  coalesce(a.order_count, 0) as order_count,
  coalesce(a.lifetime_net_revenue, 0) as lifetime_net_revenue,
  case when coalesce(a.order_count,0) = 0 then 0 else round(a.lifetime_net_revenue / a.order_count, 2) end as avg_order_value
from cust c
left join agg a
  on c.customer_id = a.customer_id
