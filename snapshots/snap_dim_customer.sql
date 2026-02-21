{% snapshot snap_dim_customer %}
{{
  config(
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
  )
}}

select
  customer_id,
  customer_name,
  customer_address,
  customer_phone,
  nation_id,
  nation_name,
  region_id,
  region_name,
  account_balance,
  market_segment,
  updated_at
from {{ ref('int_dim_customer') }}

{% endsnapshot %}