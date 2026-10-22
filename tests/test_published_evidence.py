"""Guardrails for publication accuracy and image paths."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_cloud_evidence_reconciles_historical_months():
    payload = json.loads((ROOT / "docs/evidence/cloud_execution_reference.json").read_text())
    final = payload["run_metrics"][-1]
    assert sum(payload["final_silver_by_month"].values()) == final["silver_rows"]
    assert final["bronze_rows"] == 7052769
    assert final["gold_rows"] == 13884
    assert all(row["quality_passed"] == row["quality_total"] == 8 for row in payload["run_metrics"])
    assert "predates" in payload["source"]


def test_readme_references_existing_dashboard_assets():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for file in [
        "operations_overview.png", "zone_performance.png",
        "time_patterns.png", "data_quality.png",
    ]:
        assert f"docs/images/{file}" in readme
        assert (ROOT / "docs/images" / file).is_file()
    assert "not a new cloud benchmark" in readme
