select *
from {{ ref('fct_order_revenue') }}
where net_revenue < 0
