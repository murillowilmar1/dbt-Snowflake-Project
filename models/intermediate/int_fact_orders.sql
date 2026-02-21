with o as (
  select * from {{ ref('stg_tpch_orders') }}
),
li as (
  select * from {{ ref('stg_tpch_lineitems') }}
),
agg as (
  select
    order_id,
    count(*) as line_count,
    sum(quantity) as total_quantity,
    sum(extended_price) as gross_amount,
    sum(extended_price * (1 - discount)) as net_amount,
    min(ship_date) as min_ship_date,
    max(ship_date) as max_ship_date,
    max(updated_at) as li_updated_at
  from li
  group by order_id
)

select
  o.order_id,
  o.customer_id,
  o.order_date,
  o.order_status,
  o.order_priority,
  o.total_price,

  a.line_count,
  a.total_quantity,
  a.gross_amount,
  a.net_amount,
  a.min_ship_date,
  a.max_ship_date,

  greatest(o.updated_at, a.li_updated_at) as updated_at
from o
left join agg a on o.order_id = a.order_id