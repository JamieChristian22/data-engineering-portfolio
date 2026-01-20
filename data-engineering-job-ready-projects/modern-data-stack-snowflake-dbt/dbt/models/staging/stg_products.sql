with src as (
  select * from {{ source('raw','products_raw') }}
)
select
  product_id,
  category,
  brand,
  cast(list_price as number(10,2)) as list_price,
  cast(unit_cost as number(10,2)) as unit_cost,
  cast(is_subscription as boolean) as is_subscription,
  cast(is_active as boolean) as is_active
from src
