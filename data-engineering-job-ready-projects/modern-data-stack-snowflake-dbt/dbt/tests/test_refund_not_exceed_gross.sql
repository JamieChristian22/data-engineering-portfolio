-- Fails if refunds exceed gross revenue at the order level (shouldn't happen)
select
  order_id,
  gross_revenue,
  refund_amount
from {{ ref('fct_orders') }}
where refund_amount > gross_revenue
