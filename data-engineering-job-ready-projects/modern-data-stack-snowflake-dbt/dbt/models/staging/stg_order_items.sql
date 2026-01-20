with src as (
  select * from {{ source('raw','order_items_raw') }}
)
select
  order_item_id,
  order_id,
  product_id,
  cast(quantity as number(10,0)) as quantity,
  cast(list_price as number(10,2)) as list_price,
  cast(discount_pct as number(5,4)) as discount_pct,
  cast(unit_price as number(10,2)) as unit_price,
  cast(unit_price * quantity as number(12,2)) as line_revenue
from src
