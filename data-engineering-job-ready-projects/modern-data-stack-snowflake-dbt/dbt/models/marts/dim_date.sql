-- Date spine for the project window
with date_spine as (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="to_date('" ~ var('start_date') ~ "')",
      end_date="to_date('" ~ var('end_date') ~ "')"
  ) }}
)
select
  cast(date_day as date) as date_day,
  year(date_day) as year,
  month(date_day) as month,
  monthname(date_day) as month_name,
  quarter(date_day) as quarter,
  dayofweekiso(date_day) as dow_iso,
  dayname(date_day) as day_name,
  case when dayofweekiso(date_day) in (6,7) then true else false end as is_weekend
from date_spine
