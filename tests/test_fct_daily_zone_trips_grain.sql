select
    pickup_date,
    pickup_zone,
    count(*) as row_count
from {{ ref('fct_daily_zone_trips') }}
group by 1, 2
having count(*) > 1
