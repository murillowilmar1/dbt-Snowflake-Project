{{ config(materialized='view') }}

select
  n_nationkey as nation_id,
  n_name,
  n_regionkey,
  n_comment,
  dateadd(day, mod(n_nationkey, 3), to_timestamp_ntz('2020-01-01')) as simulated_updated_at
from {{ source('tpch', 'NATION') }}