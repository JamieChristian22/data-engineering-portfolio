select
  o.order_id,
  o.customer_id,
  o.order_date,
  o.channel,
  o.device,
  o.amount_usd,
  o.status
from {{ ref('stg_orders') }} o
where o.status = 'paid'
