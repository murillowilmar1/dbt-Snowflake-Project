with n as (
  select
    nation_id,
    nation_name,
    region_id,
    updated_at
  from {{ ref('stg_tpch_nations') }}
),
r as (
  select
    region_id,
    region_name
  from {{ ref('stg_tpch_regions') }}
)

select
  {{ sk(["cast(n.nation_id as string)", "cast(n.region_id as string)"]) }} as geo_id,
  n.nation_id,
  n.nation_name,
  n.region_id,
  r.region_name,
  n.updated_at
from n
left join r on n.region_id = r.region_id