{{ config(materialized='view') }}

with base as (
  select
    c_custkey as customer_id,
    c_name,
    c_address,
    c_phone,
    c_nationkey,
    c_acctbal,
    c_mktsegment,
    c_comment
  from {{ source('tpch', 'CUSTOMER') }}
),

simulated as (
  select
    customer_id,
    -- simulamos que algunos clientes "cambian" de segment o address
    case
      when mod(customer_id, 10) = 0 then c_name || ' '
      else c_name
    end as c_name,

    case
      when mod(customer_id, 15) = 0 then '  NEW ADDRESS ' || c_address
      else c_address
    end as c_address,

    c_phone,
    c_nationkey,
    c_acctbal,

    case
      when mod(customer_id, 20) = 0 then 'AUTOMOBILE'
      else c_mktsegment
    end as c_mktsegment,

    c_comment,

    -- updated_at “determinístico” para que snapshot detecte cambios en runs distintos:
    -- en dbt Cloud puedes cambiar esta lógica a current_timestamp() si quieres forzar cambio en cada ejecución.
    dateadd(day, mod(customer_id, 5), to_timestamp_ntz('2020-01-01')) as simulated_updated_at
  from base
)

select * from simulated