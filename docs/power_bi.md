# Power BI semantic model

Use a Date dimension plus Gold zone metrics. Add zone attributes if you load the official taxi-zone lookup.

```DAX
Trips = SUM(GoldDailyZoneMetrics[trip_count])

Revenue = SUM(GoldDailyZoneMetrics[total_revenue])

Revenue per Trip = DIVIDE([Revenue], [Trips])

Average Distance =
DIVIDE(
    SUMX(GoldDailyZoneMetrics, GoldDailyZoneMetrics[avg_distance] * GoldDailyZoneMetrics[trip_count]),
    [Trips]
)

Peak Trip Share =
DIVIDE(
    SUMX(GoldDailyZoneMetrics, GoldDailyZoneMetrics[peak_trip_share] * GoldDailyZoneMetrics[trip_count]),
    [Trips]
)
```

Validate totals against the Fabric Gold table before taking screenshots.

