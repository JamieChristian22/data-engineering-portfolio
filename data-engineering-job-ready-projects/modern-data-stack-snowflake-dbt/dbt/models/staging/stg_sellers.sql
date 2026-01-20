with src as (
  select * from {{ source('raw','sellers_raw') }}
)
select
  seller_id,
  seller_tier,
  upper(seller_country_code) as seller_country_code
from src
