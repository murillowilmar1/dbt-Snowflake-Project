# dbt_snowflake_project — Snowflake TPCH (Enterprise demo)

Proyecto dbt estilo “empresa” sobre Snowflake usando datos de ejemplo:
`SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`

Este repo implementa un pipeline completo:
- limpieza robusta (staging)
- transformación y enriquecimiento (intermediate)
- dimensiones con historial (snapshots / SCD Type 2)
- modelo dimensional tipo Kimball (marts: dims + facts)
- facts incrementales con estrategia MERGE
- data testing y documentación (dbt docs)

---

## 1) Arquitectura por capas (qué significa cada una)

### A) `sources` (entrada)
Definimos las tablas raw (TPCH) como fuentes dbt. Esto permite:
- documentar origen
- tener tests básicos sobre fuentes
- usar `source()` con lineage claro

**Origen real**:
- `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER`
- `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS`
- `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM`
- `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION`
- `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION`

---

### B) `staging` (limpieza y estandarización)
Objetivo: dejar “raw” usable y consistente.

Aquí hacemos:
- trim de espacios
- upper/lower donde aplica
- parseo/normalización de fechas
- tipado fuerte (NUMBER/DATE/etc.)
- deduplicación (cuando aplica)
- columnas renombradas a nombres estables (`customer_id`, `order_date`, etc.)

Materialización típica: **views**.

---

### C) `intermediate` (enriquecer y preparar para modelo dimensional)
Objetivo: modelos “listos para negocio” pero aún no finales.

Aquí hacemos:
- joins entre entidades (ej: customer + nation + region)
- métricas base (ej: net_amount en lineitems)
- agregaciones (ej: fact_orders a nivel orden)
- lógica reusable antes de construir marts

Materialización típica: **views**.

---

### D) `snapshots` (SCD Type 2)
Objetivo: historial de cambios en dimensiones.

Snapshots crean:
- `dbt_valid_from`
- `dbt_valid_to`
- (y la tabla snapshot se va actualizando con cambios)

En este proyecto, snapshots se usan para:
- Customer (dim_customer con historial)
- Geography (dim_geography con historial)

> Nota práctica: TPCH es estático. Para un demo estable, usamos la versión vigente (`dbt_valid_to is null`) en facts.

---

### E) `marts` (modelo dimensional consumible)
Objetivo: capa final para BI/analytics.

Incluye:
- `dims/`
  - `dim_customer` (basada en snapshot)
  - `dim_geography` (basada en snapshot)
  - `dim_date` (calendario)
- `facts/`
  - `fact_sales` (línea; incremental merge)
  - `fact_orders` (orden; incremental merge)

Materialización:
- dims: views (o tables si quieres performance)
- facts: **incremental** con `merge`

---

## 2) Estructura de carpetas del proyecto

```text
dbt_snowflake_project/
├── dbt_project.yml
├── packages.yml
├── selectors.yml                # (opcional) selectors por capa
├── README.md
├── macros/
│   ├── cleaning.sql             # trim/upper/to_date_safe
│   └── surrogate_keys.sql       # sk() surrogate keys
├── models/
│   ├── staging/
│   │   ├── sources.yml          # definiciones de source()
│   │   ├── staging.yml          # tests/docs staging
│   │   └── tpch/
│   │       ├── stg_tpch_customers.sql
│   │       ├── stg_tpch_orders.sql
│   │       ├── stg_tpch_lineitems.sql
│   │       ├── stg_tpch_nations.sql
│   │       └── stg_tpch_regions.sql
│   ├── intermediate/
│   │   ├── intermediate.yml
│   │   ├── int_dim_customer.sql
│   │   ├── int_dim_geography.sql
│   │   ├── int_fact_sales.sql
│   │   └── int_fact_orders.sql
│   └── marts/
│       ├── marts.yml            # tests/docs marts
│       ├── dims/
│       │   ├── dim_customer.sql
│       │   ├── dim_geography.sql
│       │   └── dim_date.sql
│       └── facts/
│           ├── fact_sales.sql
│           └── fact_orders.sql
├── snapshots/
│   ├── snap_dim_customer.sql
│   └── snap_dim_geography.sql
└── tests/
    ├── test_dim_customer_only_one_current_version.sql
    ├── test_fact_sales_no_future_ship_date.sql
    ├── test_fact_orders_no_future_order_date.sql
    └── test_fact_sales_net_amount_non_negative.sql


---

## 3) Requisitos (Snowflake + dbt Cloud)

### Snowflake

Necesitas:

- Acceso a `SNOWFLAKE_SAMPLE_DATA`
- Un Warehouse (ej: `DBT_WH`)
- Un Database destino (ej: `SNOWFLAKE_DBT_TEST`)
- Un Schema base (ej: `DBT_SCHEMA`)

### Schemas por capa (recomendado)

dbt Cloud, cuando usas `+schema:` por capa, crea automáticamente:

- `DBT_SCHEMA_STAGING`
- `DBT_SCHEMA_INTERMEDIATE`
- `DBT_SCHEMA_MARTS`
- `DBT_SCHEMA_SNAPSHOTS`

Si no existen, debes crearlos manualmente:

```sql
create schema if not exists SNOWFLAKE_DBT_TEST.DBT_SCHEMA_STAGING;
create schema if not exists SNOWFLAKE_DBT_TEST.DBT_SCHEMA_INTERMEDIATE;
create schema if not exists SNOWFLAKE_DBT_TEST.DBT_SCHEMA_MARTS;
create schema if not exists SNOWFLAKE_DBT_TEST.DBT_SCHEMA_SNAPSHOTS;


---

# 4) Dependencias (Packages)

Este proyecto utiliza paquetes externos para extender las capacidades de dbt.

## Instalación de dependencias

```bash
dbt deps
```

## packages.yml

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: ">=1.0.0"

  - package: metaplane/dbt_expectations
    version: ">=0.10.0"
```

---

## ¿Qué aporta cada paquete?

### 🔹 dbt_utils

Utilizado para:

- `generate_surrogate_key()` → crear llaves sustitutas
- `date_spine()` → construir dim_date
- `accepted_range` → validaciones numéricas

Ejemplo:

```sql
{{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }}
```

---

### 🔹 dbt_expectations

Permite validaciones avanzadas estilo Great Expectations.

Ejemplo:

```yaml
- name: discount
  tests:
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: 0
        max_value: 1
```

---

# 5) Cómo ejecutar el pipeline

## Desarrollo por capas (modo recomendado)

Permite validar cada etapa antes de avanzar.

```bash
# 1. Staging
dbt run --select staging
dbt test --select staging

# 2. Intermediate
dbt run --select intermediate
dbt test --select intermediate

# 3. Snapshots (SCD2)
dbt snapshot

# 4. Marts
dbt run --select marts
dbt test --select marts
```

---

## Modo Enterprise (CI/CD)

Este es el comando recomendado para producción:

```bash
dbt build
```

### ¿Qué hace `dbt build`?

- Ejecuta models
- Ejecuta snapshots
- Ejecuta tests
- Respeta dependencias automáticamente

Es el equivalente a un pipeline completo.

---

# 6) Validaciones en Snowflake

## Ver schemas creados por capa

```sql
show schemas like 'DBT_SCHEMA_%' in database SNOWFLAKE_DBT_TEST;
```

Esperado:

- DBT_SCHEMA_STAGING
- DBT_SCHEMA_INTERMEDIATE
- DBT_SCHEMA_MARTS
- DBT_SCHEMA_SNAPSHOTS

---

## Validaciones básicas de conteo

```sql
select count(*) from SNOWFLAKE_DBT_TEST.DBT_SCHEMA_MARTS.FACT_SALES;
select count(*) from SNOWFLAKE_DBT_TEST.DBT_SCHEMA_MARTS.FACT_ORDERS;
select count(*) from SNOWFLAKE_DBT_TEST.DBT_SCHEMA_MARTS.DIM_CUSTOMER;
```

---

## Revenue por región

```sql
select
  g.region_name,
  sum(f.net_amount) as revenue
from SNOWFLAKE_DBT_TEST.DBT_SCHEMA_MARTS.FACT_SALES f
join SNOWFLAKE_DBT_TEST.DBT_SCHEMA_MARTS.DIM_GEOGRAPHY g
  on f.geo_scd_key = g.geo_scd_key
group by 1
order by 2 desc;
```

---

# 7) Documentación (dbt Docs)

⚠️ En dbt Cloud NO se usa `dbt docs serve`.

## Generar documentación

```bash
dbt docs generate
```

## Publicar documentación correctamente (recomendado)

Crear un Job en:

**Orchestration → Jobs**

Comandos:

```bash
dbt build
dbt docs generate
```

Después del Job exitoso:

👉 Ir a **Documentation** en el menú izquierdo.

Ahí se verá:

- Lineage completo
- Snapshots
- Tests
- Dependencias
- Exposures

---

# 8) Data Quality & Testing

## Tests genéricos (definidos en YAML)

### Not Null

```yaml
- name: order_id
  tests:
    - not_null
```

---

### Unique

```yaml
- name: sales_line_key
  tests:
    - unique
```

---

### Rango aceptado

```yaml
- name: net_amount
  tests:
    - dbt_utils.accepted_range:
        arguments:
          min_value: 0
          inclusive: true
```

---

### Validación entre valores

```yaml
- name: discount
  tests:
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: 0
        max_value: 1
```

---

## Tests singulares (SQL)

Ubicados en `/tests`

### Solo una versión vigente por customer

```sql
select customer_id
from {{ ref('dim_customer') }}
where is_current
group by customer_id
having count(*) > 1
```

---

### No fechas futuras

```sql
select *
from {{ ref('fact_sales') }}
where ship_date > current_date()
```

---

# 9) Macros utilizadas

## clean_trim

```sql
{% macro clean_trim(col) %}
  nullif(trim({{ col }}), '')
{% endmacro %}
```

Elimina espacios y convierte vacío en NULL.

---

## clean_trim_upper

```sql
{% macro clean_trim_upper(col) %}
  upper(nullif(trim({{ col }}), ''))
{% endmacro %}
```

Normaliza texto a mayúsculas.

---

## sk (Surrogate Key)

```sql
{% macro sk(cols) %}
  {{ dbt_utils.generate_surrogate_key(cols) }}
{% endmacro %}
```

Genera hash determinístico estable.

---

# 10) Próximos pasos profesionales

## A) Job de Producción programado

Ejemplo:

- Frecuencia: Diario
- Comandos:

```bash
dbt build
dbt docs generate
```

---

## B) Exposures (conectar dashboards al lineage)

Archivo: `models/marts/exposures.yml`

```yaml
version: 2

exposures:
  - name: executive_sales_dashboard
    type: dashboard
    maturity: high
    depends_on:
      - ref('fact_sales')
      - ref('dim_customer')
      - ref('dim_geography')
    owner:
      name: Data Team
      email: data@company.com
```

---

## C) CI/CD (Pull Requests)

Ejecutar solo modelos modificados:

```bash
dbt build --select state:modified+
```

Esto permite Slim CI.

---

## D) Data Contracts

Permiten forzar esquema y tipos esperados en producción.

---

# 11) Cheat Sheet

```bash
dbt deps
dbt compile
dbt build
dbt snapshot
dbt test
dbt docs generate
```

---

# Estado Final del Proyecto

✔ Arquitectura por capas  
✔ Dimensiones SCD Type 2  
✔ Facts incrementales con MERGE  
✔ Surrogate Keys  
✔ Testing robusto  
✔ Documentación automática  
✔ Preparado para CI/CD  
✔ Estructura enterprise real  

---