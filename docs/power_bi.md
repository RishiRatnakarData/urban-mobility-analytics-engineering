# Power BI Desktop semantic model

Download the Gold and latest quality CSV exports created by the Databricks notebook. Load
`gold_daily_zone_metrics.csv` as `GoldDailyZoneMetrics` and
`data_quality_results_latest.csv` as `DataQualityResults`. Use a Date dimension plus Gold zone
metrics. Add names and boroughs only if you also load the official taxi-zone lookup.

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

## Four report pages

1. **Operations Overview:** Trips, Revenue, Revenue per Trip, Average Distance, and daily trend.
2. **Zone Performance:** pickup-location ranking, trips, revenue, and distance.
3. **Time Patterns:** daily trend and peak-trip share; add hourly visuals only from a validated
   trip-level export.
4. **Data Quality:** check name, passed status, observed value, and last checked time.

Validate totals against the Databricks Gold Delta table before taking screenshots. Power BI evidence
is incomplete until the `.pbix` is built personally and the rendered values match the source.

