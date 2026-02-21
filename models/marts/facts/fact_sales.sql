{{
  config(
    materialized='incremental',
    unique_key='sales_line_key',
    incremental_strategy='merge'
  )
}}

with base as (
  select * from {{ ref('int_fact_sales') }}
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
    {{ sk(["cast(base.order_id as string)", "cast(base.line_number as string)"]) }} as sales_line_key,

    base.order_id,
    base.line_number,
    base.customer_id,
    base.order_date,
    base.ship_date,
    base.commit_date,
    base.receipt_date,

    base.quantity,
    base.extended_price,
    base.discount,
    base.tax,
    base.net_amount,

    base.return_flag,
    base.line_status,
    base.ship_mode,
    base.ship_instruct,

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
where ship_date > (select coalesce(max(ship_date), '1900-01-01'::date) from {{ this }})
{% endif %}