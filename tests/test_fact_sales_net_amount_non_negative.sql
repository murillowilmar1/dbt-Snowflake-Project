select *
from {{ ref('fact_sales') }}
where net_amount < 0