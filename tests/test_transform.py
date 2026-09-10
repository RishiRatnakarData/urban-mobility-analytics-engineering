import pandas as pd

from src.transform import (
    quality_checks,
    refresh_gold_for_dates,
    to_gold,
    to_silver,
    upsert_silver,
)


def test_invalid_and_duplicate_rows_are_removed():
    raw = pd.DataFrame({
        "trip_id": ["1", "1", "2", "3"],
        "pickup_datetime": ["2025-01-01 10:00", "2025-01-01 10:00", "2025-01-01 12:00", "bad"],
        "dropoff_datetime": ["2025-01-01 10:20", "2025-01-01 10:20", "2025-01-01 11:55", "2025-01-01 13:00"],
        "pickup_zone": ["A", "A", "B", "C"],
        "dropoff_zone": ["B", "B", "A", "D"],
        "passenger_count": [1, 1, 2, 1],
        "trip_distance": [2.0, 2.0, 1.0, 3.0],
        "fare_amount": [10.0, 10.0, 8.0, 12.0],
        "tip_amount": [2.0, 2.0, 1.0, 2.0],
    })
    silver = to_silver(raw)
    assert list(silver["trip_id"]) == ["1"]
    assert silver.iloc[0]["revenue"] == 12.0
    assert quality_checks(raw, silver)["passed"].all()


def test_gold_has_expected_grain():
    raw = pd.DataFrame({
        "trip_id": ["1", "2"],
        "pickup_datetime": ["2025-01-01 10:00", "2025-01-01 11:00"],
        "dropoff_datetime": ["2025-01-01 10:20", "2025-01-01 11:30"],
        "pickup_zone": ["A", "A"],
        "dropoff_zone": ["B", "C"],
        "passenger_count": [1, 2],
        "trip_distance": [2.0, 3.0],
        "fare_amount": [10.0, 15.0],
        "tip_amount": [2.0, 3.0],
    })
    gold = to_gold(to_silver(raw))
    assert len(gold) == 1
    assert gold.iloc[0]["trip_count"] == 2
    assert gold.iloc[0]["total_revenue"] == 30.0


def test_late_arrival_upserts_trip_and_refreshes_historical_date():
    initial_raw = pd.DataFrame({
        "trip_id": ["1", "2"],
        "pickup_datetime": ["2025-01-02 10:00", "2025-01-03 11:00"],
        "dropoff_datetime": ["2025-01-02 10:20", "2025-01-03 11:30"],
        "pickup_zone": ["A", "B"],
        "dropoff_zone": ["B", "C"],
        "passenger_count": [1, 2],
        "trip_distance": [2.0, 3.0],
        "fare_amount": [10.0, 15.0],
        "tip_amount": [2.0, 3.0],
    })
    current_silver = to_silver(initial_raw)
    current_gold = to_gold(current_silver)
    late_raw = pd.DataFrame({
        "trip_id": ["3", "1"],
        "pickup_datetime": ["2025-01-02 12:00", "2025-01-02 10:00"],
        "dropoff_datetime": ["2025-01-02 12:15", "2025-01-02 10:20"],
        "pickup_zone": ["A", "A"],
        "dropoff_zone": ["D", "B"],
        "passenger_count": [1, 1],
        "trip_distance": [1.5, 2.0],
        "fare_amount": [9.0, 11.0],
        "tip_amount": [1.0, 2.0],
    })

    merged = upsert_silver(current_silver, late_raw)
    affected_dates = sorted(to_silver(late_raw)["pickup_date"].unique())
    refreshed = refresh_gold_for_dates(current_gold, merged, affected_dates)

    assert merged["trip_id"].is_unique
    assert merged.loc[merged["trip_id"] == "1", "revenue"].item() == 13.0
    jan_2 = refreshed[refreshed["pickup_date"] == affected_dates[0]]
    assert jan_2["trip_count"].sum() == 2
    assert jan_2["total_revenue"].sum() == 23.0

