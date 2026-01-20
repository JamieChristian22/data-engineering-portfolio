with orders as (
  select * from {{ ref('stg_orders') }}
),
items as (
  select * from {{ ref('stg_order_items') }}
),
refunds as (
  select
    order_id,
    sum(refund_amount) as refund_amount
  from {{ ref('stg_refunds') }}
  group by 1
),
order_rollup as (
  select
    o.order_id,
    o.customer_id,
    o.order_ts,
    date_trunc('day', o.order_ts) as order_date,
    date_trunc('month', o.order_ts) as order_month,
    o.channel,
    o.traffic_source,
    o.campaign,
    o.payment_method,
    o.shipping_method,
    o.order_status,
    o.seller_id,
    sum(i.line_revenue) as gross_revenue,
    sum(i.quantity) as total_units,
    count(distinct i.product_id) as distinct_products
  from orders o
  join items i
    on o.order_id = i.order_id
  group by 1,2,3,4,5,6,7,8,9,10,11,12
),
final as (
  select
    r.*,
    coalesce(f.refund_amount, 0) as refund_amount,
    r.gross_revenue - coalesce(f.refund_amount, 0) as net_revenue
  from order_rollup r
  left join refunds f
    on r.order_id = f.order_id
)
select * from final
