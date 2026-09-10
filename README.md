# Urban Mobility Analytics Engineering Platform

[![CI](https://github.com/RishiRatnakarData/urban-mobility-analytics-engineering/actions/workflows/ci.yml/badge.svg)](https://github.com/RishiRatnakarData/urban-mobility-analytics-engineering/actions/workflows/ci.yml)

Mobility operations teams need daily zone metrics they can trust after duplicate, invalid, rerun,
and late-arriving records enter the pipeline. This project implements a testable Bronze/Silver/Gold
contract locally and provides a Databricks PySpark/Delta implementation for authenticated execution.

> **Portfolio status:** validated locally and through authenticated Databricks Free Edition
> execution. The cloud build processed official NYC TLC January and February 2025 Parquet files,
> created five Delta tables, passed all eight quality checks, proved idempotency and controlled
> recovery, handled a January-after-February late arrival, and produced a four-page Power BI report.

## Current verified result

Refactor validation performed on September 10, 2026:

| Check | Verified result |
|---|---|
| Python transformation tests | 3/3 passed, including a late-arrival/upsert case |
| Sample Bronze / Silver / Gold rows | 12 / 12 / 12 |
| Local persisted quality checks | 6/6 passed |
| dbt DuckDB build | 2 models and 8 tests passed; 10/10 nodes |
| Environment | Python 3.12.10, dbt Core 1.12.4, dbt-duckdb 1.9.4 |

The committed fixture is intentionally tiny. These results verify behavior and reproducibility, not
real-world traffic volume. Cloud-scale evidence is recorded separately below.

## Authenticated Databricks result

Execution performed on Databricks Free Edition on September 10, 2026 UTC:

| Scenario | Bronze rows | Silver rows | Gold rows | Quality | Refreshed dates | Duration |
|---|---:|---:|---:|---:|---:|---:|
| February 2025 initial load | 3,577,543 | 3,306,910 | 6,668 | 8/8 | 30 | 86.66 s |
| February idempotent rerun | 3,577,543 | 3,306,910 | 6,668 | 8/8 | 30 | 163.55 s |
| Recovery after missing-source test | 3,577,543 | 3,306,910 | 6,668 | 8/8 | 30 | 149.60 s |
| January processed after February | 7,052,769 | 6,560,019 | 13,884 | 8/8 | 33 | 150.97 s |

The late-arrival result contains 3,253,109 January Silver rows and 3,306,910 February Silver rows.
The controlled test raised `FileNotFoundError` before table writes, and the repaired run returned to
the same February business-table counts. On rerun, `trip_id_unique` and `gold_grain_unique` remained
passing while audit history stayed append-only.

## What the project demonstrates

- A raw-to-curated medallion design with explicit grain and ownership at every layer.
- Schema validation, invalid-row filtering, deterministic keys, and duplicate protection.
- Persisted data-quality and pipeline-run evidence rather than console-only assertions.
- Delta MERGE for repeatable Bronze/Silver ingestion in the Databricks notebook.
- Partition-scoped Gold refreshes for late-arriving historical trips.
- dbt staging and an incremental daily-zone mart with a 62-day lookback and tested grain.
- Cloud-independent CI for Python, notebook syntax, the sample pipeline, and dbt.
- A documented Power BI Desktop semantic model based on small curated exports.

## Architecture

```mermaid
flowchart TD
    A[Official TLC Parquet] --> B[Databricks serverless notebook]
    B --> C[Bronze Delta]
    C --> D[PySpark Silver Delta]
    D --> E[Quality and run logs]
    D --> F[Partitioned Gold Delta]
    F --> G[Power BI exports]
    H[Local fixture] --> I[Pandas, Parquet, DuckDB, dbt]
    I --> J[GitHub Actions]
```

See [`docs/architecture.md`](docs/architecture.md) for layer grains, design decisions, and the
separation between local and cloud evidence.

## Reproduce the local contract

### Windows PowerShell

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m pytest -q
python -m src.pipeline --sample
dbt build --profiles-dir .
```

### macOS or Linux

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m pytest -q
python -m src.pipeline --sample
dbt build --profiles-dir .
```

The sample command persists Bronze, Silver, and Gold Parquet outputs, six quality results, and a JSON
run summary. Generated data and artifacts are ignored by Git.

## Execute on Databricks

Follow the [Databricks Free Edition checklist](docs/databricks_build.md). It covers:

1. An official NYC TLC monthly Parquet source.
2. Notebook import and managed-volume upload.
3. Five Delta tables and eight cloud quality checks.
4. Same-file rerun/idempotency validation.
5. A controlled missing-path failure and successful recovery.
6. A January-after-February late-arrival demonstration.
7. Curated CSV exports for Power BI Desktop.

Databricks Free Edition is sufficient for this portfolio execution; an Azure subscription is not
part of this repository's runtime.

## Claims ledger

| Claim | Evidence |
|---|---|
| Local Python tests | `3/3 passed` |
| Local sample rows | `Bronze 12; Silver 12; Gold 12` |
| Local quality checks | `6/6 passed` |
| Local dbt result | `2 models + 8 tests; 10/10 nodes` |
| Databricks run date | `2026-09-10 UTC` |
| Official source | `NYC TLC Yellow Taxi 2025-02: 60,343,086 bytes; 2025-01: 59,158,238 bytes` |
| Databricks business rows | `After both months: Bronze 7,052,769; Silver 6,560,019; Gold 13,884` |
| Databricks quality checks | `8/8 passed` |
| Pipeline duration | `86.66 s initial; 163.55 s rerun; 149.60 s recovery; 150.97 s late arrival` |
| Idempotent rerun | `Counts unchanged at 3,577,543 / 3,306,910 / 6,668; uniqueness checks passed` |
| Controlled recovery | `Missing-path FileNotFoundError occurred before writes; repaired run passed 8/8` |
| Late-arriving data | `January processed after February; 33 partitions refreshed; both months retained` |
| Power BI | `6,560,019 trips; $138,813,614.92 revenue; $21.16/trip; 5.68 mi; 37.57% peak share` |
| GitHub Actions | `Pending first public push` |

## Power BI report

![Urban Mobility Operations Overview](docs/images/operations_overview.png)

Additional pages: [Zone Performance](docs/images/zone_performance.png) ·
[Time Patterns](docs/images/time_patterns.png) · [Data Quality](docs/images/data_quality.png)

## Repository map

```text
databricks/          importable PySpark/Delta source notebook
src/                 local medallion and late-arrival contract
models/              dbt staging and incremental serving mart
tests/               transformation, late-arrival, and dbt-grain tests
docs/                architecture, execution, Power BI, and interview guidance
data/sample/         small committed fixture for deterministic CI
.github/workflows/   lint, tests, sample execution, and dbt build
```

## Evidence boundaries and limitations

- The sample fixture is synthetic-style validation data, not representative of city operations.
- The official cloud source is NYC taxi data; the broader title refers to the transferable pipeline
  pattern, not a claim that multiple cities are currently loaded.
- Local pandas/DuckDB results do not prove Databricks execution or workspace permissions.
- The deterministic trip key can theoretically collide when distinct trips share its input fields.
- A production system would require orchestrated jobs, incremental file discovery, quarantine rules,
  alert delivery, access control, environment promotion, and cost/performance testing.
- Taxi activity does not represent every travel mode and may reflect geographic and service-access
  biases.

## Interview preparation

Use [`docs/interview_guide.md`](docs/interview_guide.md) to practice the five-minute story and explain
idempotency, late arrivals, layer responsibilities, dbt's role, and the limits of CI.

## Author

Rishi Ratnakar â€” [LinkedIn](https://www.linkedin.com/in/rishi-ratnakar) |
[GitHub](https://github.com/RishiRatnakarData)
