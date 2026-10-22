"""Regression tests for late changes, cross-layer reconciliation, and quality gating."""

from __future__ import annotations

import pandas as pd
import pytest

from src.quality_contract import validated_quality_checks
from src.transform import (
    affected_dates_for_upsert, quality_checks, refresh_gold_for_dates,
    to_gold, to_silver, upsert_silver,
)


def batch(*, ids=("A", "B"), dates=("2025-01-02", "2025-01-03"), fares=(10.0, 20.0)):
    return pd.DataFrame({
        "trip_id": list(ids),
        "pickup_datetime": [f"{date} 09:00" for date in dates],
        "dropoff_datetime": [f"{date} 09:15" for date in dates],
        "pickup_zone": ["Midtown"] * len(ids),
        "dropoff_zone": ["Downtown"] * len(ids),
        "passenger_count": [1] * len(ids),
        "trip_distance": [2.0] * len(ids),
        "fare_amount": list(fares),
        "tip_amount": [2.0] * len(ids),
    })


def test_same_batch_twice_has_one_silver_record_per_trip():
    raw = batch()
    once = upsert_silver(pd.DataFrame(), raw)
    twice = upsert_silver(once, raw)
    pd.testing.assert_frame_equal(once, twice)


def test_updated_trip_moves_to_new_date_and_removes_old_gold_partition():
    initial = to_silver(batch(ids=("A",), dates=("2025-01-02",), fares=(10.0,)))
    gold = to_gold(initial)
    corrected = batch(ids=("A",), dates=("2025-01-07",), fares=(20.0,))
    incoming = to_silver(corrected)
    affected = affected_dates_for_upsert(initial, incoming)
    assert affected == [pd.Timestamp("2025-01-02").date(), pd.Timestamp("2025-01-07").date()]
    merged = upsert_silver(initial, corrected)
    refreshed = refresh_gold_for_dates(gold, merged, affected)
    assert len(refreshed) == 1
    assert list(refreshed["pickup_date"]) == [pd.Timestamp("2025-01-07").date()]
    assert refreshed.iloc[0]["total_revenue"] == 22.0


def test_late_historical_batch_preserves_unaffected_newer_partition():
    current = to_silver(batch())
    original_gold = to_gold(current)
    late = batch(ids=("C",), dates=("2024-12-30",), fares=(8.0,))
    incoming = to_silver(late)
    merged = upsert_silver(current, late)
    gold = refresh_gold_for_dates(original_gold, merged, affected_dates_for_upsert(current, incoming))
    assert set(gold["pickup_date"]) == {
        pd.Timestamp("2024-12-30").date(),
        pd.Timestamp("2025-01-02").date(),
        pd.Timestamp("2025-01-03").date(),
    }
    assert int(gold["trip_count"].sum()) == 3


def test_invalid_latest_duplicate_does_not_erase_valid_trip():
    raw = batch(ids=("A", "A"), dates=("2025-01-02", "2025-01-02"))
    raw.loc[1, "trip_distance"] = -5
    silver = to_silver(raw)
    assert len(silver) == 1
    assert silver.loc[0, "fare_amount"] == 10.0


@pytest.mark.parametrize("column,value", [
    ("trip_distance", float("inf")),
    ("trip_distance", float("-inf")),
    ("fare_amount", float("nan")),
    ("fare_amount", float("inf")),
    ("trip_id", None),
    ("trip_id", ""),
])
def test_invalid_identifiers_and_nonfinite_values_are_filtered(column, value):
    raw = batch(ids=("A",), dates=("2025-01-02",), fares=(10.0,))
    raw.loc[0, column] = value
    assert to_silver(raw).empty


def test_missing_columns_fail_closed():
    with pytest.raises(ValueError, match="Missing required columns"):
        to_silver(batch().drop(columns=["trip_id"]))


def test_quality_gate_detects_gold_revenue_and_counts_mismatch():
    raw = batch()
    silver = to_silver(raw)
    gold = to_gold(silver)
    checks = validated_quality_checks(raw, silver, gold)
    assert checks["passed"].all()
    assert len(checks) == len(quality_checks(raw, silver)) + 4
    corrupted = gold.copy()
    corrupted.loc[0, "total_revenue"] += 4
    corrupted.loc[0, "trip_count"] += 3
    checks = validated_quality_checks(raw, silver, corrupted).set_index("check_name")
    assert not bool(checks.loc["gold_revenue_reconciled", "passed"])
    assert not bool(checks.loc["gold_trip_counts_reconciled", "passed"])


def test_gold_grain_duplicate_is_detected():
    raw = batch()
    silver = to_silver(raw)
    gold = to_gold(silver)
    bad = pd.concat([gold, gold], ignore_index=True)
    checks = validated_quality_checks(raw, silver, bad).set_index("check_name")
    assert not bool(checks.loc["gold_grain_unique", "passed"])


def test_all_invalid_rows_are_rejected_before_persistence(tmp_path, monkeypatch):
    from src import pipeline
    monkeypatch.setattr(pipeline, "ROOT", tmp_path)
    raw = batch(ids=("A",), dates=("2025-01-02",), fares=(10.0,))
    raw["trip_distance"] = -1.0
    path = tmp_path / "invalid.csv"
    raw.to_csv(path, index=False)
    with pytest.raises(ValueError, match="No valid trips"):
        pipeline.run_batch(path)
    assert not (tmp_path / "data" / "silver" / "trips.parquet").exists()


def test_lineage_counts_new_and_existing_matches():
    from src.lineage import upsert_counts
    current = to_silver(batch())
    incoming = to_silver(batch(ids=("A", "C"), dates=("2025-01-02", "2025-01-04")))
    metrics = upsert_counts(current, incoming, 3)
    assert metrics["new_trip_ids"] == 1
    assert metrics["existing_trip_ids_matched"] == 1
    assert metrics["incoming_filtered_or_duplicate_rows"] == 1


def test_pipeline_rerun_with_parquet_stubs(tmp_path, monkeypatch):
    """Tests cross-run logic without requiring an installed Parquet engine in the harness."""
    from src import pipeline

    monkeypatch.setattr(pipeline, "ROOT", tmp_path)
    original_read = pd.read_parquet
    original_write = pd.DataFrame.to_parquet

    def read_stub(path, *args, **kwargs):
        return pd.read_pickle(path)

    def write_stub(self, path, *args, **kwargs):
        self.to_pickle(path)

    monkeypatch.setattr(pd, "read_parquet", read_stub)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", write_stub)
    try:
        source = tmp_path / "batch1.csv"
        batch(ids=("A",), dates=("2025-01-02",), fares=(10.0,)).to_csv(source, index=False)
        result = pipeline.run_batch(source)
        assert result["quality_checks_passed"] == 10
        assert result["upsert"]["new_trip_ids"] == 1
        rerun = pipeline.run_batch(source)
        assert rerun["upsert"]["existing_trip_ids_matched"] == 1
        correction = tmp_path / "batch2.csv"
        batch(ids=("A",), dates=("2025-01-07",), fares=(20.0,)).to_csv(correction, index=False)
        result = pipeline.run_batch(correction)
        assert len(result["affected_dates"]) == 2
        gold = pd.read_pickle(tmp_path / "data/gold/daily_zone_metrics.parquet")
        assert len(gold) == 1
        assert float(gold.loc[0, "total_revenue"]) == 22
        history = (tmp_path / "artifacts/run_history.jsonl").read_text().strip().splitlines()
        assert len(history) == 3
    finally:
        monkeypatch.setattr(pd, "read_parquet", original_read)
        monkeypatch.setattr(pd.DataFrame, "to_parquet", original_write)
