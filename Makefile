.PHONY: help fetch normalize publish verify all clean install test update status

help:  ## Show this help message
	@echo "Hangarbay Makefile"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:  ## Install package in development mode
	pip install -e ".[dev]"

update:  ## Update all data (fetch, normalize, publish)
	hangar update

status:  ## Show data status and age
	hangar status

fetch:  ## Download latest FAA registry files
	python -m pipelines.fetch

normalize:  ## Normalize raw files to Parquet
	python -m pipelines.normalize

publish:  ## Build DuckDB and SQLite from Parquet
	python -m pipelines.publish

verify:  ## Run data quality checks (not yet implemented)
	@echo "verify: coming soon"

all: fetch normalize publish verify  ## Run full pipeline

test:  ## Run tests
	pytest tests/ -v

clean:  ## Remove generated files (keeps raw data)
	rm -rf data/interim/* data/publish/*
	rm -rf dist/ build/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

clean-all: clean  ## Remove all data including raw downloads
	rm -rf data/

