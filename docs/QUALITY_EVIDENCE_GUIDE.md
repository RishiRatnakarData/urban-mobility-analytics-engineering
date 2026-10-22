# Historical cloud quality evidence — figure provenance

`docs/images/cloud_quality_evidence.png` is a **derived, presentation-oriented summary** of the historical Databricks quality run recorded in [`cloud_execution_reference.json`](evidence/cloud_execution_reference.json). It is not a Power BI page, not an image of live cloud execution, and not a new October 2026 benchmark.

The figure uses the **January late-arrival final run** on September 10, 2026: 7,052,769 Bronze records, 6,560,019 Silver trips, 13,884 Gold daily-zone rows, and 8/8 passed validations. Check names are transcribed into plain English from the original published Power BI check table; they describe nonempty Bronze/Silver, trip-ID completeness/uniqueness, location completeness, positive duration/distance, and unique Gold grain. The existing screenshot [`data_quality.png`](images/data_quality.png) has **not** been edited or overwritten.

The figure is meant to be readable at GitHub gallery size; the unaltered Power BI screenshot and complete [Databricks execution notes](databricks_execution.md) provide the original dashboard and run details. The separate local Python pipeline was validated with a 12-row fixture and **10 local quality checks**, not the cloud dataset.
