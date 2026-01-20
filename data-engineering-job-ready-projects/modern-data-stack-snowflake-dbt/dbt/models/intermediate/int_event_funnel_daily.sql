with events as (
  select * from {{ ref('stg_events') }}
),
daily as (
  select
    date_trunc('day', event_ts) as event_date,
    channel,
    device,
    traffic_source,
    campaign,
    count_if(event_type = 'page_view') as page_views,
    count_if(event_type = 'view_item') as view_item,
    count_if(event_type = 'add_to_cart') as add_to_cart,
    count_if(event_type = 'begin_checkout') as begin_checkout,
    count_if(event_type = 'purchase') as purchase,
    count_if(event_type = 'refund') as refund,
    count(distinct session_id) as sessions,
    count(distinct customer_id) as customers
  from events
  group by 1,2,3,4,5
)
select * from daily
