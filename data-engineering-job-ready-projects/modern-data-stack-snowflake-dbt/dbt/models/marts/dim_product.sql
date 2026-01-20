select
  product_id,
  category,
  brand,
  list_price,
  unit_cost,
  is_subscription,
  is_active
from {{ ref('stg_products') }}
