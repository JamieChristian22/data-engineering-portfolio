with source as (
  select * from raw.customers
)
select
  customer_id,
  created_date,
  state,
  city,
  email_domain
from source
