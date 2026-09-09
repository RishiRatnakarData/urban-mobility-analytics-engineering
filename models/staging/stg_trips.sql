select
    cast(trip_id as varchar) as trip_id,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(pickup_zone as varchar) as pickup_zone,
    cast(dropoff_zone as varchar) as dropoff_zone,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as double) as trip_distance,
    cast(fare_amount as double) as fare_amount,
    cast(tip_amount as double) as tip_amount,
    cast(revenue as double) as revenue,
    cast(trip_minutes as double) as trip_minutes,
    cast(pickup_date as date) as pickup_date,
    cast(is_peak as integer) as is_peak
from read_parquet('data/silver/trips.parquet')

