with daily as (
  select * from {{ ref('fct_orders') }}
)
select
  order_date,
  sum(net_revenue) as net_revenue,
  sum(gross_revenue) as gross_revenue,
  sum(refund_amount) as refunds,
  sum(total_units) as units,
  count(*) as orders,
  case when count(*)=0 then 0 else round(sum(net_revenue)/count(*),2) end as aov
from daily
where order_status = 'completed'
group by 1
