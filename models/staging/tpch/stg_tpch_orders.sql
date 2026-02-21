with src as (
    select * from {{ source('tpch', 'ORDERS') }}
),

typed as (
    select
        o_orderkey::number as order_id,
        o_custkey::number as customer_id,
        {{ to_date_safe('o_orderdate') }} as order_date,
        {{ clean_trim_upper('o_orderstatus') }} as order_status,
        o_totalprice::number(18,2) as total_price,
        {{ clean_trim_upper('o_orderpriority') }} as order_priority,
        {{ clean_trim_upper('o_clerk') }} as clerk,
        o_shippriority::number as ship_priority,
        {{ clean_trim_upper('o_comment') }} as comment,
        current_timestamp() as updated_at
    from src
),

deduped as (
    select *
    from typed
    qualify row_number() over (
        partition by order_id
        order by updated_at desc
    ) = 1
)

select * from deduped
