with s as (
  select
    *,
    {{ sk(["geo_id", "cast(dbt_valid_from as string)"]) }} as geo_scd_key,
    case when dbt_valid_to is null then true else false end as is_current
  from {{ ref('snap_dim_geography') }}
)

select * from s