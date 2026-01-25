with o as (select * from {{ ref('stg_orders') }}),
l as (select * from {{ ref('stg_order_lines') }}),
p as (select * from {{ ref('stg_products') }})

select
  o.order_id,
  o.order_ts::date as order_date,
  date_trunc('month', o.order_ts)::date as order_month,
  o.customer_id,
  o.channel,
  o.status,
  p.category,
  p.brand,
  sum(l.quantity * l.unit_price * (1 - l.discount_rate)) as net_revenue,
  sum(l.quantity * l.unit_cost) as cogs,
  sum(l.quantity) as units
from o
join l on l.order_id = o.order_id
join p on p.sku = l.sku
group by 1,2,3,4,5,6,7,8
