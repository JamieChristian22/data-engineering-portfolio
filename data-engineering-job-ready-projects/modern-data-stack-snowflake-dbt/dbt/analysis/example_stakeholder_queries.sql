-- Example stakeholder questions you can answer immediately in Snowflake:

-- 1) Revenue by category (net) last 30 days
select
  date_trunc('month', order_date) as month,
  category,
  sum(line_revenue) as gross_revenue,
  sum(line_gross_profit) as gross_profit
from {{ ref('fct_order_items') }}
where order_status = 'completed'
group by 1,2
order by 1,2;

-- 2) Conversion rate by traffic source (sessions -> purchase)
select
  event_date,
  traffic_source,
  sum(sessions) as sessions,
  sum(purchase) as purchases,
  case when sum(sessions)=0 then 0 else round(sum(purchase)/sum(sessions),4) end as conversion_rate
from {{ ref('fct_events_daily') }}
group by 1,2
order by 1,2;

-- 3) Top 20 customers by lifetime net revenue
select
  customer_id,
  lifetime_net_revenue,
  order_count,
  avg_order_value
from {{ ref('int_customer_ltv') }}
order by lifetime_net_revenue desc
limit 20;
