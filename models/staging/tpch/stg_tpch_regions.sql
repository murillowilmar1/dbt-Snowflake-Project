with src as (
    select * from {{ source('tpch', 'REGION') }}
),

typed as (
    select
        r_regionkey::number as region_id,
        {{ clean_trim_upper('r_name') }} as region_name,
        {{ clean_trim_upper('r_comment') }} as comment,
        current_timestamp() as updated_at
    from src
),

deduped as (
    select *
    from typed
    qualify row_number() over (
        partition by region_id
        order by updated_at desc
    ) = 1
)

select * from deduped
