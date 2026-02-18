with src as (
    select * from {{ source('tpch', 'LINEITEM') }}
),

typed as (
    select
        l_orderkey::number as order_id,
        l_linenumber::number as line_number,
        l_partkey::number as part_id,
        l_suppkey::number as supplier_id,
        l_quantity::number(18,2) as quantity,
        l_extendedprice::number(18,2) as extended_price,
        l_discount::number(18,4) as discount,
        l_tax::number(18,4) as tax,
        {{ clean_trim_upper('l_returnflag') }} as return_flag,
        {{ clean_trim_upper('l_linestatus') }} as line_status,
        {{ to_date_safe('l_shipdate') }} as ship_date,
        {{ to_date_safe('l_commitdate') }} as commit_date,
        {{ to_date_safe('l_receiptdate') }} as receipt_date,
        {{ clean_trim_upper('l_shipinstruct') }} as ship_instruct,
        {{ clean_trim_upper('l_shipmode') }} as ship_mode,
        {{ clean_trim_upper('l_comment') }} as comment,
        current_timestamp() as updated_at
    from src
),

deduped as (
    select *
    from typed
    qualify row_number() over (
        partition by order_id, line_number
        order by updated_at desc
    ) = 1
)

select * from deduped
