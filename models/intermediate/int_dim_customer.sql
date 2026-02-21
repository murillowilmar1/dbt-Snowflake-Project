with c as (
  select * from {{ ref('stg_tpch_customers') }}
),
n as (
  select
    nation_id,
    nation_name,
    region_id
  from {{ ref('stg_tpch_nations') }}
),
r as (
  select
    region_id,
    region_name
  from {{ ref('stg_tpch_regions') }}
)

select
  c.customer_id,
  c.customer_name,
  c.customer_address,
  c.customer_phone,
  c.nation_id,
  n.nation_name,
  n.region_id,
  r.region_name,
  c.account_balance,
  c.market_segment,
  c.updated_at
from c
left join n on c.nation_id = n.nation_id
left join r on n.region_id = r.region_id