.PHONY: install test lint sample dbt-build all
install:
	python -m pip install -r requirements.txt
test:
	pytest -q
lint:
	ruff check src tests
sample:
	python -m src.pipeline --sample
dbt-build:
	dbt build --profiles-dir .
all: test sample dbt-build
