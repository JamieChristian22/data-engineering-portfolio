with paid as (
  select * from {{ ref('fct_orders') }}
),
agg as (
  select
    order_date as date,
    channel,
    device,
    count(*) as orders,
    sum(amount_usd)::numeric(12,2) as revenue_usd
  from paid
  group by 1,2,3
)
select * from agg
order by date, channel, device
