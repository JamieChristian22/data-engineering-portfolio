select
  customer_id,
  signup_date,
  country_code,
  region,
  is_b2b,
  plan_tier
from {{ ref('stg_customers') }}
