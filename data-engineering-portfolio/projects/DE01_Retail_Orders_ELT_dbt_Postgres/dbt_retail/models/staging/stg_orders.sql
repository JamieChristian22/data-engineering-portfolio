with src as (
  select * from raw.orders
),
typed as (
  select
    order_id,
    order_ts::timestamp as order_ts,
    customer_id::int as customer_id,
    channel,
    payment_method,
    status,
    ship_state
  from src
)
select * from typed
