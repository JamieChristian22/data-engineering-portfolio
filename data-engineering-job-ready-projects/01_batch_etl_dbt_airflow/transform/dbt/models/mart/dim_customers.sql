select
  c.customer_id,
  c.created_date,
  c.state,
  c.city,
  c.email_domain,
  -- customer tenure in days
  (current_date - c.created_date) as tenure_days
from {{ ref('stg_customers') }} c
