with src as (
  select * from raw.customers
)
select
  customer_id::int as customer_id,
  lower(email) as email,
  first_name,
  last_name,
  signup_date::date as signup_date,
  state,
  segment
from src
