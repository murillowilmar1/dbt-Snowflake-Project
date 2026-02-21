{% snapshot snap_dim_geography %}
{{
  config(
    unique_key='geo_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
  )
}}

select
  geo_id,
  nation_id,
  nation_name,
  region_id,
  region_name,
  updated_at
from {{ ref('int_dim_geography') }}

{% endsnapshot %}