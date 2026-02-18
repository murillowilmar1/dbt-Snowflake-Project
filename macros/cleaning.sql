{% macro clean_trim(col) -%}
    nullif(trim({{ col }}), '')
{%- endmacro %}

{% macro clean_trim_upper(col) -%}
    upper(nullif(trim({{ col }}), ''))
{%- endmacro %}

{% macro to_date_safe(col) -%}
    try_to_date({{ col }})
{%- endmacro %}

{% macro to_timestamp_safe(col) -%}
    try_to_timestamp_ntz({{ col }})
{%- endmacro %}
