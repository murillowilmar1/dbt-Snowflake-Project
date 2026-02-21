select *
from {{ ref('fact_sales') }}
where ship_date > current_date()