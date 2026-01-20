select
  seller_id,
  seller_tier,
  seller_country_code
from {{ ref('stg_sellers') }}
