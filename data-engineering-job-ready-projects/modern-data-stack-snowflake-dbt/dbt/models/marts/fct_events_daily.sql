select
  event_date,
  channel,
  device,
  traffic_source,
  campaign,
  sessions,
  customers,
  page_views,
  view_item,
  add_to_cart,
  begin_checkout,
  purchase,
  refund,
  case when sessions = 0 then 0 else round(purchase / sessions, 4) end as session_conversion_rate
from {{ ref('int_event_funnel_daily') }}
