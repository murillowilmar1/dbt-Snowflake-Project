select *
from {{ ref('fact_orders') }}
where order_date > current_date()