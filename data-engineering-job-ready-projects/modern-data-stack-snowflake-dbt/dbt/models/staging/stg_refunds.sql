with src as (
  select * from {{ source('raw','refunds_raw') }}
)
select
  refund_id,
  order_id,
  order_item_id,
  refund_ts,
  cast(refund_amount as number(12,2)) as refund_amount,
  refund_reason
from src
