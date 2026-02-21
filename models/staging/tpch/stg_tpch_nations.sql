with src as (
    select * from {{ source('tpch', 'NATION') }}
),

typed as (
    select
        n_nationkey::number as nation_id,
        {{ clean_trim_upper('n_name') }} as nation_name,
        n_regionkey::number as region_id,
        {{ clean_trim_upper('n_comment') }} as comment,
        current_timestamp() as updated_at
    from src
),

deduped as (
    select *
    from typed
    qualify row_number() over (
        partition by nation_id
        order by updated_at desc
    ) = 1
)

select * from deduped
