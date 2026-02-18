with src as (
    select * from {{ source('tpch', 'CUSTOMER') }}
),

typed as (
    select
        c_custkey::number as customer_id,
        {{ clean_trim_upper('c_name') }} as customer_name,
        {{ clean_trim_upper('c_address') }} as customer_address,
        {{ clean_trim('c_phone') }} as customer_phone,
        c_nationkey::number as nation_id,
        c_acctbal::number(18,2) as account_balance,
        {{ clean_trim_upper('c_mktsegment') }} as market_segment,
        {{ clean_trim_upper('c_comment') }} as comment,
        current_timestamp() as updated_at
    from src
),

deduped as (
    select *
    from typed
    qualify row_number() over (
        partition by customer_id
        order by updated_at desc
    ) = 1
)

select * from deduped
