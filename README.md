# Urban Mobility Analytics Engineering Platform

[![CI](https://github.com/RishiRatnakarData/urban-mobility-analytics-engineering/actions/workflows/ci.yml/badge.svg)](https://github.com/RishiRatnakarData/urban-mobility-analytics-engineering/actions/workflows/ci.yml)

A production-shaped mobility pipeline using Bronze/Silver/Gold modeling, explicit data-quality evidence, PySpark/Fabric implementation, dbt marts, automated tests, and a Power BI serving design.

> Portfolio status: local sample implementation included. Change this to `Fabric implementation complete` only after following `docs/fabric_build.md` in your own workspace.

## What the current local build proves

- Reproducible sample ingestion with explicit Bronze, Silver, and Gold responsibilities.
- Python transformation and validation behavior covered by two automated tests.
- Twelve sample records processed through all three local layers with six quality checks passing.
- Two dbt models and seven data tests completed successfully in DuckDB.
- A cloud-independent local contract that reviewers can run without Microsoft Fabric access.

## What still requires execution evidence

- An authenticated Microsoft Fabric run using an official NYC TLC monthly Parquet file.
- PySpark execution, Delta-table persistence, rerun/idempotency validation, and failure recovery.
- A completed Power BI semantic model and four-page report.
- A successful GitHub Actions run after the repository is published.

## Local build

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
pytest -q
python -m src.pipeline --sample
dbt build --profiles-dir .
```

This creates local Bronze, Silver, and Gold Parquet outputs, six persisted quality checks, and a DuckDB dbt warehouse. It allows reviewers to verify the logic without a Fabric account.

### Verified local evidence

Validation performed on September 9, 2026:

| Check | Result |
|---|---|
| Python tests | 2/2 passed |
| Sample Bronze/Silver/Gold rows | 12 / 12 / 12 |
| Sample quality checks | 6/6 passed |
| dbt build | 2 models and 7 tests passed; 9/9 total |
| Environment | Python 3.12.10, dbt Core 1.12.4, dbt-duckdb 1.9.4 |

## Fabric build

Follow the [exact Fabric checklist](docs/fabric_build.md), then replace the placeholder below with your real screenshot.

![Fabric evidence placeholder](docs/images/architecture_placeholder.svg)

## Architecture

See [architecture and design decisions](docs/architecture.md).

## Claims ledger - replace with actual evidence

| Claim | Evidence |
|---|---|
| Source and month | `[NYC TLC URL and YYYY-MM]` |
| Bronze rows | `[exact count]` |
| Silver valid unique trips | `[exact count]` |
| Gold rows and grain | `[count; date x pickup zone]` |
| Quality checks | `[passed / total]` |
| Pipeline run time | `[minutes/seconds]` |
| Rerun/idempotency result | `[exact observation]` |
| Failure-recovery test | `[failure injected and repair]` |
| dbt result | `Local DuckDB build: 2 models and 7 tests passed; 9/9 total` |

## Limitations

- The committed data is a tiny synthetic-style fixture for CI, not representative of NYC operations.
- Local pandas/DuckDB execution validates logic but is not a substitute for a Fabric integration run.
- A production platform needs incremental ingestion, late-arrival rules, observability alerts, access control, cost tests, and deployment environments.
- Taxi demand does not represent all mobility demand and can reflect geographic and service-access biases.

## Repository map

```text
src/                 local reproducible medallion pipeline
fabric/              paste-ready Fabric PySpark notebook source
models/              dbt staging and incremental mart
tests/               transformation and grain checks
docs/                architecture, Fabric, and Power BI instructions
.github/workflows/   automated local build and tests
```

## Author

Rishi Ratnakar - [LinkedIn](https://www.linkedin.com/in/rishi-ratnakar) | [GitHub](https://github.com/RishiRatnakarData)

