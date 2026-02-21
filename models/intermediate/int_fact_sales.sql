with li as (
  select * from {{ ref('stg_tpch_lineitems') }}
),
o as (
  select * from {{ ref('stg_tpch_orders') }}
)

select
  li.order_id,
  li.line_number,
  o.customer_id,
  o.order_date,
  li.ship_date,
  li.commit_date,
  li.receipt_date,

  li.quantity,
  li.extended_price,
  li.discount,
  li.tax,
  (li.extended_price * (1 - li.discount)) as net_amount,

  li.return_flag,
  li.line_status,
  li.ship_mode,
  li.ship_instruct,

  greatest(li.updated_at, o.updated_at) as updated_at
from li
join o on li.order_id = o.order_id