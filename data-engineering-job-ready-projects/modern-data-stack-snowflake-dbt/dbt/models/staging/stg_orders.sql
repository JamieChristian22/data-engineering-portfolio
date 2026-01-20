with src as (
  select * from {{ source('raw','orders_raw') }}
)
select
  order_id,
  customer_id,
  order_ts,
  channel,
  traffic_source,
  campaign,
  payment_method,
  shipping_method,
  order_status,
  nullif(seller_id, '') as seller_id
from src
