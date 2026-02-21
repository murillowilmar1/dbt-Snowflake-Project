with s as (
  select
    *,
    {{ sk(["cast(customer_id as string)", "cast(dbt_valid_from as string)"]) }} as customer_scd_key,
    case when dbt_valid_to is null then true else false end as is_current
  from {{ ref('snap_dim_customer') }}
)

select * from s