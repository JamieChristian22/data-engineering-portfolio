with src as (
  select * from {{ source('raw','customers_raw') }}
)
select
  customer_id,
  signup_date,
  upper(country_code) as country_code,
  region,
  cast(is_b2b as boolean) as is_b2b,
  plan_tier
from src
