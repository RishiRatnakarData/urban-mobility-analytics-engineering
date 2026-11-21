"""Offline local-pipeline observability tests without a live cloud runtime."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pandas as pd
import pytest

from src.observability import build_report

NOW = datetime(2026, 11, 20, 19, 0, tzinfo=UTC)


@pytest.fixture
def snapshot(tmp_path, monkeypatch):
    # The production implementation reads Parquet. Pickle is a temporary
    # testing substitute to avoid requiring external Parquet engines here.
    monkeypatch.setattr(pd, "read_parquet", lambda path, columns=None: pd.read_pickle(path)[columns])
    silver_dir = tmp_path / "data/silver"
    gold_dir = tmp_path / "data/gold"
    artifacts = tmp_path / "artifacts"
    for directory in (silver_dir, gold_dir, artifacts):
        directory.mkdir(parents=True)
    silver = pd.DataFrame({
        "trip_id": ["A", "B"],
        "pickup_datetime": ["2025-01-01 09:00", "2025-01-02 09:00"],
        "pickup_zone": ["Midtown", "Midtown"],
        "dropoff_zone": ["Downtown", "Airport"],
    })
    gold = pd.DataFrame({
        "pickup_date": ["2025-01-01", "2025-01-02"],
        "pickup_zone": ["Midtown", "Midtown"],
        "trip_count": [1, 1],
    })
    silver.to_pickle(silver_dir / "trips.parquet")
    gold.to_pickle(gold_dir / "daily_zone_metrics.parquet")
    summary = {
        "run_utc": "2026-11-20T18:45:00+00:00",
        "bronze_batch_rows": 3,
        "silver_rows": 2,
        "gold_rows": 2,
        "quality_checks_total": 2,
        "quality_checks_passed": 2,
        "processing_duration_seconds": 1.2717,
        "upsert": {
            "incoming_valid_unique_trips": 2,
            "incoming_filtered_or_duplicate_rows": 1,
        },
    }
    (artifacts / "run_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (artifacts / "last_attempt.json").write_text(json.dumps({
        "run_utc": "2026-11-20T18:45:02+00:00", "status": "success"
    }), encoding="utf-8")
    pd.DataFrame({
        "check_name": ["silver_unique", "gold_unique"],
        "passed": [True, True],
        "observed": [0, 0],
    }).to_csv(artifacts / "data_quality_results.csv", index=False)
    return tmp_path


def test_good_snapshot_reports_accounting_quality_and_duration(snapshot):
    output = build_report(snapshot, now=NOW)
    assert output["status"] == "passed"
    assert output["errors"] == []
    assert output["batch_counts"]["last_rejected_or_duplicate_rows"] == 1
    assert output["batch_counts"]["persisted_silver_rows"] == 2
    assert output["processing_duration_seconds"] == pytest.approx(1.272)
    assert output["data_freshness"]["newest_pickup_date"] == "2025-01-02"
    assert output["current_snapshot"]["silver_duplicate_trip_ids"] == 0
    assert output["quality_checks"]["checks_passed"] == 2


def test_opt_in_historical_freshness_warning(snapshot):
    default = build_report(snapshot, now=NOW)
    assert not any("trip data exceeds" in warning for warning in default["warnings"])
    flagged = build_report(snapshot, now=NOW, max_data_age_days=20)
    assert flagged["status"] == "passed"
    assert any("trip data exceeds" in warning for warning in flagged["warnings"])
    flagged = build_report(snapshot, now=NOW, max_run_age_hours=0.01)
    assert any("successful run exceeds" in warning for warning in flagged["warnings"])


def test_detects_missing_and_null_values(snapshot):
    path = snapshot / "data/silver/trips.parquet"
    frame = pd.read_pickle(path)
    frame.loc[0, "pickup_zone"] = None
    frame.to_pickle(path)
    result = build_report(snapshot, now=NOW)
    assert result["status"] == "failed_validation"
    assert result["current_snapshot"]["silver_key_nulls"]["pickup_zone"] == 1
    assert any("null" in error for error in result["errors"])


def test_detects_duplicates_and_reconciliation_failure(snapshot):
    path = snapshot / "data/silver/trips.parquet"
    frame = pd.read_pickle(path)
    frame.loc[1, "trip_id"] = "A"
    frame.to_pickle(path)
    gold_path = snapshot / "data/gold/daily_zone_metrics.parquet"
    gold = pd.read_pickle(gold_path)
    gold.loc[1, "trip_count"] = 4
    gold.to_pickle(gold_path)
    result = build_report(snapshot, now=NOW)
    assert result["status"] == "failed_validation"
    assert result["current_snapshot"]["silver_duplicate_trip_ids"] == 1
    assert any("reconcile" in err for err in result["errors"])


def test_failed_attempt_overrides_prior_success(snapshot):
    (snapshot / "artifacts/last_attempt.json").write_text(json.dumps({
        "run_utc": "2026-11-20T18:59:00+00:00",
        "status": "failed", "error_type": "ValueError",
    }), encoding="utf-8")
    result = build_report(snapshot, now=NOW)
    assert result["status"] == "failed_validation"
    assert result["last_attempt"]["error_type"] == "ValueError"
    assert any("attempt failed" in err for err in result["errors"])


def test_old_summary_is_not_reported_as_last_successful_attempt(snapshot):
    (snapshot / "artifacts/last_attempt.json").unlink()
    summary_path = snapshot / "artifacts/run_summary.json"
    raw = json.loads(summary_path.read_text())
    raw.pop("processing_duration_seconds")
    summary_path.write_text(json.dumps(raw), encoding="utf-8")
    result = build_report(snapshot, now=NOW)
    assert result["status"] == "snapshot_passed_latest_attempt_unknown"
    assert result["processing_duration_seconds"] is None
    assert any("not recorded" in v for v in result["warnings"])


def test_missing_files_and_invalid_qualities(snapshot):
    (snapshot / "data/silver/trips.parquet").unlink()
    result = build_report(snapshot, now=NOW)
    assert result["status"] == "failed_validation"
    assert any("Missing persisted" in v for v in result["errors"])
    csv = snapshot / "artifacts/data_quality_results.csv"
    csv.write_text("check_name,passed,observed\ncheck,maybe,0\n", encoding="utf-8")
    with pytest.raises(ValueError, match="pass flags"):
        build_report(snapshot, now=NOW)


def test_bad_row_counts_and_timestamps_fail(snapshot):
    p = snapshot / "artifacts/run_summary.json"
    original = json.loads(p.read_text())
    changed = {**original, "bronze_batch_rows": -1}
    p.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(ValueError, match="row count"):
        build_report(snapshot, now=NOW)
    changed = {**original, "run_utc": "nonsense"}
    p.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(ValueError, match="timestamp"):
        build_report(snapshot, now=NOW)


def test_bad_threshold_or_time_fails(snapshot):
    with pytest.raises(ValueError, match="nonnegative"):
        build_report(snapshot, max_data_age_days=-1)
    with pytest.raises(ValueError, match="timezone"):
        build_report(snapshot, now=datetime(2026, 11, 20))


def test_cli_writes_auditable_json(snapshot, monkeypatch):
    import sys
    from src.observability import main

    output = snapshot / "artifacts/observability.json"
    monkeypatch.setattr(sys, "argv", [
        "observability", "--root", str(snapshot), "--out", str(output)
    ])
    main()
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["status"] == "passed"
    assert result["scope"] == "local_pandas_parquet_outputs_only_not_databricks"
