{{
  config(
    materialized='incremental',
    unique_key='customer_sk'
  )
}}

with src as (
  select
    customer_id,
    email,
    first_name,
    last_name,
    signup_date,
    state,
    segment,
    md5(concat_ws('|', customer_id::text, state, segment)) as version_hash
  from {{ ref('stg_customers') }}
),

prep as (
  select
    customer_id,
    email,
    first_name,
    last_name,
    signup_date,
    state,
    segment,
    version_hash,
    current_timestamp as valid_from,
    null::timestamp as valid_to,
    true as is_current
  from src
),

-- current records in target
current_tgt as (
  select * from {{ this }}
  where is_current = true
)

select
  md5(concat_ws('|', p.customer_id::text, p.version_hash)) as customer_sk,
  p.customer_id,
  p.email,
  p.first_name,
  p.last_name,
  p.signup_date,
  p.state,
  p.segment,
  p.version_hash,
  p.valid_from,
  p.valid_to,
  p.is_current
from prep p
{% if is_incremental() %}
left join current_tgt t
  on t.customer_id = p.customer_id
where t.customer_id is null
   or t.version_hash <> p.version_hash
{% endif %}
