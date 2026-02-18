# dbt_snowflake_power (TPCH sample data)

Proyecto dbt "empresa" sobre Snowflake usando `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`.

## Capas
- staging: limpieza, normalización, deduplicación
- intermediate: joins, llaves surrogate, preparación
- snapshots: SCD Type 2 para dimensiones
- marts:
  - dims: dim_customer, dim_geography, dim_date
  - facts: fact_sales (línea) y fact_orders (orden), incrementales

## Requisitos
- Snowflake warehouse/db/schema creados:
  - WAREHOUSE: DBT_WH (o el tuyo)
  - DATABASE: DBT_DEMO
  - SCHEMA: ANALYTICS

## Instalación
```bash
pip install dbt-snowflake
dbt deps
