with source as (
  select * from raw.orders
)
select
  order_id,
  customer_id,
  order_date,
  channel,
  device,
  amount_usd,
  status
from source
