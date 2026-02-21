{{ config(materialized='table') }}

with spine as (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="to_date('1992-01-01')",
      end_date="dateadd(day, 1, current_date())"
  ) }}
)

select
  date_day,
  year(date_day) as year,
  month(date_day) as month,
  day(date_day) as day,
  dayofweekiso(date_day) as day_of_week_iso,
  weekofyear(date_day) as week_of_year,
  quarter(date_day) as quarter,
  to_char(date_day, 'YYYY-MM') as year_month,
  last_day(date_day) as month_end_date,
  case when dayofweekiso(date_day) in (6,7) then true else false end as is_weekend
from spine