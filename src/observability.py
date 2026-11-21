"""Read-only observability report over persisted local medallion artifacts.

Historical taxi-data freshness is descriptive unless an explicit threshold is set.
The report does not operate or claim to monitor Databricks cloud services.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


def _read_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return data


def _as_utc(value: object, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid {field} timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must contain a timezone offset")
    return parsed.astimezone(UTC)


def _safe_count(value: object, name: str) -> int:
    try:
        number = int(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError(f"Invalid {name} row count") from exc
    if number < 0 or str(value) != str(number):
        raise ValueError(f"Invalid {name} row count")
    return number


def build_report(
    root: Path,
    *,
    now: datetime | None = None,
    max_data_age_days: float | None = None,
    max_run_age_hours: float | None = None,
) -> dict[str, object]:
    """Audit an existing local Silver/Gold snapshot and its latest attempt log."""
    if max_data_age_days is not None and max_data_age_days < 0:
        raise ValueError("max_data_age_days must be nonnegative")
    if max_run_age_hours is not None and max_run_age_hours < 0:
        raise ValueError("max_run_age_hours must be nonnegative")
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("now must have a timezone")
    now = now.astimezone(UTC)
    root = root.resolve()
    artifact = root / "artifacts"
    summary = _read_json(artifact / "run_summary.json")
    attempt = _read_json(artifact / "last_attempt.json")
    silver_path = root / "data/silver/trips.parquet"
    gold_path = root / "data/gold/daily_zone_metrics.parquet"
    quality_path = artifact / "data_quality_results.csv"
    errors: list[str] = []
    warnings: list[str] = []

    batch_counts = None
    run_age_hours = None
    duration = None
    if summary is None:
        errors.append("No persisted successful run summary found")
    else:
        input_rows = _safe_count(summary.get("bronze_batch_rows"), "bronze_batch_rows")
        silver_rows = _safe_count(summary.get("silver_rows"), "silver_rows")
        gold_rows = _safe_count(summary.get("gold_rows"), "gold_rows")
        upsert = summary.get("upsert", {})
        if not isinstance(upsert, dict):
            raise ValueError("Invalid upsert accounting object")
        filtered = _safe_count(
            upsert.get("incoming_filtered_or_duplicate_rows"), "filtered_rows"
        )
        valid = _safe_count(
            upsert.get("incoming_valid_unique_trips"), "incoming_valid_unique_trips"
        )
        if filtered + valid != input_rows:
            errors.append("Incoming batch accounting does not reconcile")
        batch_counts = {
            "last_bronze_batch_rows": input_rows,
            "last_valid_unique_trips": valid,
            "last_rejected_or_duplicate_rows": filtered,
            "persisted_silver_rows": silver_rows,
            "persisted_gold_rows": gold_rows,
        }
        recorded_time = _as_utc(summary.get("run_utc"), field="run_utc")
        run_age_hours = round((now - recorded_time).total_seconds() / 3600, 3)
        if max_run_age_hours is not None and run_age_hours > max_run_age_hours:
            warnings.append("Most recent successful run exceeds configured age threshold")
        duration = summary.get("processing_duration_seconds")
        if duration is not None:
            if not isinstance(duration, (int, float)) or not 0 <= duration < float("inf"):
                raise ValueError("Invalid processing_duration_seconds")
            duration = round(float(duration), 3)
        else:
            warnings.append("Processing duration not recorded in this historical run")

    row_snapshot: dict[str, object] | None = None
    freshness: dict[str, object] | None = None
    if not silver_path.is_file() or not gold_path.is_file():
        errors.append("Missing persisted Silver or Gold Parquet output")
    else:
        silver = pd.read_parquet(silver_path, columns=[
            "trip_id", "pickup_datetime", "pickup_zone", "dropoff_zone",
        ])
        gold = pd.read_parquet(gold_path, columns=[
            "pickup_date", "pickup_zone", "trip_count",
        ])
        if silver.empty or gold.empty:
            errors.append("Silver or Gold persisted output is empty")
        silver_nulls = {
            field: int(silver[field].isna().sum())
            for field in ["trip_id", "pickup_datetime", "pickup_zone", "dropoff_zone"]
        }
        duplicates_silver = int(silver["trip_id"].duplicated().sum())
        duplicates_gold = int(gold.duplicated(subset=["pickup_date", "pickup_zone"]).sum())
        gold_trip_count = pd.to_numeric(gold["trip_count"], errors="coerce")
        if gold_trip_count.isna().any() or (gold_trip_count < 0).any():
            errors.append("Gold contains missing or negative trip counts")
        elif int(gold_trip_count.sum()) != len(silver):
            errors.append("Gold trip count does not reconcile with persisted Silver")
        if any(silver_nulls.values()):
            errors.append("Important Silver columns contain null values")
        if duplicates_silver or duplicates_gold:
            errors.append("Persisted Silver trip IDs or Gold daily-zone keys are duplicated")
        row_snapshot = {
            "silver_rows_observed": len(silver),
            "gold_rows_observed": len(gold),
            "silver_key_nulls": silver_nulls,
            "silver_duplicate_trip_ids": duplicates_silver,
            "gold_duplicate_daily_zone_keys": duplicates_gold,
            "gold_trip_count_sum": int(gold_trip_count.sum()) if gold_trip_count.notna().all() else None,
        }
        if summary and (len(silver) != summary["silver_rows"] or len(gold) != summary["gold_rows"]):
            errors.append("Snapshot row counts differ from latest successful summary")
        pickup = pd.to_datetime(silver["pickup_datetime"], errors="coerce")
        if pickup.isna().any():
            errors.append("Invalid pickup datetime in persisted Silver")
        elif not silver.empty:
            # NYC source timestamps are local/naive. Calendar-day freshness avoids
            # inventing a UTC offset for historical trip timestamps.
            newest = pickup.max().date()
            age_days = (now.date() - newest).days
            freshness = {
                "newest_pickup_date": newest.isoformat(),
                "newest_pickup_calendar_age_days": age_days,
                "data_age_threshold_days": max_data_age_days,
            }
            if age_days < 0:
                warnings.append("Newest trip pickup date lies in the future relative to report time")
            if max_data_age_days is not None and age_days > max_data_age_days:
                warnings.append("Persisted trip data exceeds configured age threshold")

    quality = None
    if not quality_path.is_file():
        errors.append("Persisted data-quality results CSV is missing")
    else:
        checks = pd.read_csv(quality_path)
        if not {"check_name", "passed", "observed"}.issubset(checks.columns):
            raise ValueError("Data quality CSV has invalid columns")
        flags = checks["passed"].astype(str).str.casefold()
        if checks.empty or not flags.isin({"true", "false"}).all():
            raise ValueError("Data quality CSV contains missing/invalid pass flags")
        failed_names = checks.loc[flags.eq("false"), "check_name"].astype(str).tolist()
        quality = {
            "checks_total": len(checks),
            "checks_passed": int(flags.eq("true").sum()),
            "failed_check_names": failed_names,
        }
        if failed_names:
            errors.append("One or more persisted quality checks failed")
        if summary and (
            len(checks) != summary.get("quality_checks_total")
            or quality["checks_passed"] != summary.get("quality_checks_passed")
        ):
            errors.append("Persisted quality-check totals differ from run summary")

    attempt_info = {"status": "not_recorded", "run_utc": None}
    if attempt is not None:
        status = attempt.get("status")
        if status not in {"success", "failed"}:
            raise ValueError("Invalid last_attempt status")
        attempt_time = _as_utc(attempt.get("run_utc"), field="last_attempt.run_utc")
        attempt_info = {"status": status, "run_utc": attempt_time.isoformat()}
        if status == "failed":
            errors.append("Latest instrumented CLI pipeline attempt failed")
            attempt_info["error_type"] = attempt.get("error_type")
        elif summary and attempt_time < _as_utc(summary["run_utc"], field="run_utc"):
            warnings.append("Attempt status predates the successful run summary")
    else:
        warnings.append("Latest attempted status unavailable (older runs or direct run_batch calls)")

    if errors:
        status = "failed_validation"
    elif attempt_info["status"] == "success":
        status = "passed"
    else:
        status = "snapshot_passed_latest_attempt_unknown"
    return {
        "scope": "local_pandas_parquet_outputs_only_not_databricks",
        "generated_utc": now.isoformat(),
        "status": status,
        "last_attempt": attempt_info,
        "run_age_hours": run_age_hours,
        "processing_duration_seconds": duration,
        "batch_counts": batch_counts,
        "current_snapshot": row_snapshot,
        "data_freshness": freshness,
        "quality_checks": quality,
        "errors": errors,
        "warnings": warnings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--out", type=Path, default=Path("artifacts/observability.json"))
    parser.add_argument("--max-data-age-days", type=float)
    parser.add_argument("--max-run-age-hours", type=float)
    args = parser.parse_args()
    report = build_report(
        args.root,
        max_data_age_days=args.max_data_age_days,
        max_run_age_hours=args.max_run_age_hours,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Pipeline observability: {report['status']}")
    print(f"Validation errors: {len(report['errors'])}; warnings: {len(report['warnings'])}")
    print(f"Report: {args.out}")


if __name__ == "__main__":
    main()
