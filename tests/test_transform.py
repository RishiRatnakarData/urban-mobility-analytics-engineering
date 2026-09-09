import pandas as pd

from src.transform import quality_checks, to_gold, to_silver


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

