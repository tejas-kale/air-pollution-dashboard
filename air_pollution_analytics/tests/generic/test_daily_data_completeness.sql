{% test daily_data_completeness(model, timestamp_column, city_column, threshold=0.75) %}

with daily_counts as (
    select
        date({{ timestamp_column }}) as measurement_date,
        {{ city_column }} as city,
        count(*) as measurements_per_day
    from {{ model }}
    group by 1, 2
),

completeness_check as (
    select *
    from daily_counts
    where measurements_per_day < (24 * {{ threshold }})  -- Less than threshold% of expected daily measurements
)

select *
from completeness_check

{% endtest %} 