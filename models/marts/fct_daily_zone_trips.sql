{{ config(materialized='incremental', unique_key=['pickup_date', 'pickup_zone']) }}

select
    pickup_date,
    pickup_zone,
    count(*) as trip_count,
    round(sum(revenue), 2) as total_revenue,
    round(avg(trip_distance), 2) as avg_distance,
    round(avg(trip_minutes), 2) as avg_trip_minutes,
    round(avg(is_peak), 4) as peak_trip_share
from {{ ref('stg_trips') }}
{% if is_incremental() %}
where pickup_date >= (select coalesce(max(pickup_date), date '1900-01-01') from {{ this }})
{% endif %}
group by 1, 2

