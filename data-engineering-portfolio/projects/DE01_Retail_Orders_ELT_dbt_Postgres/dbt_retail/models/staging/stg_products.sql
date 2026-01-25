select
  sku,
  category,
  brand,
  unit_cost::numeric(12,2) as unit_cost,
  unit_price::numeric(12,2) as unit_price
from raw.products
