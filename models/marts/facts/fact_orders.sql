{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
  )
}}

with base as (
  select * from {{ ref('int_fact_orders') }}
),

cust_scd as (
  select
    customer_id,
    {{ sk(["cast(customer_id as string)", "cast(dbt_valid_from as string)"]) }} as customer_scd_key,
    nation_id,
    region_id
  from {{ ref('snap_dim_customer') }}
  where dbt_valid_to is null
),

geo_scd as (
  select
    nation_id,
    region_id,
    {{ sk([ "cast(nation_id as string)", "cast(region_id as string)", "cast(dbt_valid_from as string)" ]) }} as geo_scd_key
  from {{ ref('snap_dim_geography') }}
  where dbt_valid_to is null
),

joined as (
  select
    {{ sk(["cast(base.order_id as string)"]) }} as order_fact_key,

    base.order_id,
    base.customer_id,
    base.order_date,
    base.order_status,
    base.order_priority,
    base.total_price,

    base.line_count,
    base.total_quantity,
    base.gross_amount,
    base.net_amount,
    base.min_ship_date,
    base.max_ship_date,

    cs.customer_scd_key,
    gs.geo_scd_key,

    current_timestamp() as updated_at
  from base

  left join cust_scd cs
    on cast(base.customer_id as number) = cast(cs.customer_id as number)

  left join geo_scd gs
    on cast(cs.nation_id as number) = cast(gs.nation_id as number)
   and cast(cs.region_id as number) = cast(gs.region_id as number)
)

select * from joined

{% if is_incremental() %}
where order_date > (select coalesce(max(order_date), '1900-01-01'::date) from {{ this }})
{% endif %}