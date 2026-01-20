{% snapshot snp_products_scd2 %}

{{
  config(
    target_schema='transform',
    unique_key='product_id',
    strategy='timestamp',
    updated_at='updated_at_ts'
  )
}}

with base as (
  select
    product_id,
    category,
    brand,
    list_price,
    unit_cost,
    is_subscription,
    is_active,
    current_timestamp() as updated_at_ts
  from {{ ref('stg_products') }}
)
select * from base

{% endsnapshot %}
