with src as (
  select * from {{ source('raw','events_raw') }}
)
select
  event_id,
  nullif(customer_id,'') as customer_id,
  session_id,
  event_ts,
  device,
  channel,
  traffic_source,
  campaign,
  event_type,
  nullif(product_id,'') as product_id,
  nullif(order_id,'') as order_id
from src
