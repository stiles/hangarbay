# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Changed
- **Major UI improvements** to `hangar search` output:
  - Owner and location now shown first (most interesting info at top)
  - Human-readable dates (e.g., "May 19, 2023" instead of "2023-05-19 00:00:00")
  - Decoded ALL status codes from FAA data dictionary (29 numeric codes + 12 letter codes)
  - Decoded certificate types: "Standard Airworthiness - Normal" instead of "1N", "Experimental - Amateur Built" instead of "42"
  - Expanded field names ("Year Manufactured" vs "Year Mfr", "Certificate Type" vs "Registration Type")
  - Clean structure with colons and proper spacing
  - Combined Make & Model on single line
- **Improved `hangar sql` output formatting**:
  - Replaced `<NA>` and `NaT` with empty strings for cleaner display
  - Stripped timestamps from dates (show "2023-05-19" instead of "2023-05-19 00:00:00")
  - Cleaner, more professional table output
- Added comprehensive code lookups based on official FAA data dictionary (`data/reference/ardata.txt`)
  - 41 status codes (including all pending, expired, revoked states)
  - 40+ certificate type codes (Standard, Experimental, Light Sport, Restricted, etc.)

### Added
- **🐍 Python API for notebooks and scripts:**
  - `hb.load_data()` - One-command setup (fetch, normalize, publish)
  - `hb.search(n_number)` - Look up aircraft by N-number
  - `hb.fleet(owner, state=None, limit=0)` - Find all aircraft owned by person/company
  - `hb.query(sql)` - Execute custom SQL queries
  - `hb.get_connection()` - Direct database access for advanced users
  - `hb.status()` - Check data age and counts
  - `hb.list_tables()` - Show available tables
  - `hb.schema(table)` - Show table schema
  - All functions return pandas DataFrames
  - Auto-downloads data on first use with helpful messages
  - Data stored in `~/.hangarbay/data/` by default (configurable with `set_data_dir()`)
- **New commands:**
  - `hangar fleet <owner>` - Find all aircraft owned by a person or company
    - Case-insensitive search with wildcards (automatic `%term%` wrapping)
    - **OR logic**: Use pipe separator for multiple patterns: `"LAPD|Los Angeles Police"`
    - Displays summary statistics (count, manufacturers, status)
    - Shows aircraft list with N-numbers, make/model, year, location
    - Filter by state: `--state CA`
    - Export to CSV: `--export fleet.csv`
    - Limit results: `--limit 20` (default: unlimited)
    - Warning shown when results are truncated by limit
  - `hangar update` - Run full pipeline (fetch → normalize → publish) in one command
  - `hangar status` - Check data age and show warning if stale (30+ days)
- **Data age warnings:**
  - `hangar search` and `hangar sql` now show warnings if data is 30+ days old
  - Prompts users to run `hangar update` to refresh data
  - Add `--skip-age-check` flag to disable warnings (useful for scripts)
- **Reference tables** for code lookups:
  - `status_codes` - All 41 registration status codes with descriptions
  - `airworthiness_classes` - 9 certificate classes with descriptions
  - `owner_types` - 8 owner type codes with descriptions
- **Decoded views** for convenient querying:
  - `aircraft_decoded` - Aircraft table with all codes decoded and make/model joined
  - `owners_clean` - Simplified owner table with only standardized fields and decoded owner type
- `hangar sql` now supports `--case-insensitive` (`-i`) flag to automatically convert `LIKE` to `ILIKE` for case-insensitive text matching
  - Example: `hangar sql "SELECT * FROM owners WHERE owner_name_std LIKE '%boeing%'" -i`

### Fixed
- `hangar search` now accepts N-numbers with or without the "N" prefix (e.g., both "N221LA" and "221LA" work)
- Case-insensitive search (e.g., "n100", "N100", "100" all work)

## [0.3.0] - 2025-11-08

### Added
- **Publish pipeline** (`pipelines/publish.py`) ✅
  - Loads Parquet files into DuckDB (`registry.duckdb`)
  - Creates 6 tables: aircraft, registrations, owners, aircraft_make_model, engines, owners_summary
  - Builds indexes on n_number and join keys for fast lookups
  - Creates SQLite FTS5 index (`owners.sqlite`) for owner name/address search
  - Writes publish metadata with database sizes
- **CLI query commands** (`hangarbay/cli.py`) ✅
  - `hangar sql` - Execute SQL queries with pretty table output
  - `hangar search` - Look up aircraft by N-number with owner info
  - Output formats: table (default), json, csv
  - Read-only mode by default for safety
- Makefile: `make publish` target
- Updated dependencies: added pandas

### Results
- **DuckDB**: 106 MB with 6 tables and indexes
- **SQLite FTS**: 55 MB with full-text search
- **Query performance**: Sub-second on 300K+ rows
- **Total storage**: 203 MB (vs 275 MB raw)

### Example queries
```sql
-- Total aircraft
SELECT COUNT(*) FROM aircraft;  -- 307,793

-- Top manufacturers
SELECT maker, COUNT(*) FROM aircraft JOIN aircraft_make_model USING(mfr_mdl_code)
WHERE maker != '' GROUP BY maker ORDER BY 2 DESC LIMIT 10;
-- CESSNA: 72,811, PIPER: 44,784, BEECH: 17,511

-- NETJETS fleet
SELECT COUNT(*) FROM owners WHERE owner_name_std LIKE '%NETJETS%';  -- 56

-- Top states
SELECT state_std, COUNT(*) FROM owners GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
-- TX: 28,811, CA: 25,262, FL: 21,346
```

## [0.2.0] - 2025-11-08

### Added
- **Normalize pipeline** (`pipelines/normalize.py`) ✅
  - Parses MASTER.txt into aircraft, registrations and owners tables
  - Parses ACFTREF.txt into aircraft_make_model table
  - Parses ENGINE.txt into engines table
  - Applies lite address standardization (uppercase, trim, combine, normalize state/ZIP)
  - Generates deterministic owner_id using xxhash64
  - Handles FAA date format (YYYYMMDD integers)
  - Casts all tables to Arrow schemas with type safety
  - Writes 5 Parquet files to data/publish/
  - Creates normalize.json metadata with row counts
- Address standardization utilities (`hangarbay/address.py`)
  - clean_text, standardize_state, standardize_zip
  - combine_address, standardize_owner_name
  - Comprehensive test coverage
- CLI: `hangar normalize` command working
- Makefile: `make normalize` target

### Fixed
- Column name mapping for FAA files (TYPE AIRCRAFT vs AIRWORTHINESS CLASS)
- Date parsing from YYYYMMDD integer format
- Numeric field cleaning (year_mfr with spaces)
- String field type coercion in reference tables

### Results
- **307,793** aircraft records normalized
- **307,793** registration records
- **307,793** owner records with raw + standardized addresses  
- **93,342** aircraft make/model references
- **4,736** engine specifications
- Total: **36 MB** of typed Parquet files (from 200 MB raw)

## [0.1.0] - 2025-11-08

### Added
- Initial project scaffolding with modern Python packaging (`pyproject.toml`)
- Arrow schemas for all core tables with deterministic hash generation
  - `aircraft`, `registrations`, `owners`, `aircraft_make_model`, `engines`
  - Optional: `deregistrations`, `owners_summary`
- Fetch pipeline (`pipelines/fetch.py`)
  - Downloads FAA ReleasableAircraft.zip with browser headers to avoid blocking
  - Extracts MASTER.txt, ACFTREF.txt, ENGINE.txt
  - Creates manifest.json with SHA256 hashes, timestamps and schema versions
  - Detects previous snapshots for diff tracking
  - Retry logic with exponential backoff
- CLI (`hangar`) with Typer and Rich output
  - `hangar fetch` - download latest FAA data
  - `hangar version` - show version info
  - Stubs for `search`, `fleet`, `owners`, `sql` commands
- Makefile with common targets
  - `make fetch`, `make install`, `make test`, `make clean`
- Test suite with pytest
  - Schema validation tests
  - Deterministic hash tests
  - Field presence tests
- Project documentation
  - README.md with quick start guide
  - FAA_registry_plan.md (detailed architecture plan)
  - LICENSE (MIT)
  - .gitignore for Python projects
- Directory structure
  - `hangarbay/` - package code
  - `pipelines/` - fetch, normalize, publish modules
  - `tests/` - test suite
  - `data/raw/`, `data/interim/`, `data/publish/` - data storage
  - `lookups/` - reference table storage
  - `docs/` - documentation

### Fixed
- FAA server blocking automated downloads - added browser-like headers to requests

### Notes
- Successfully fetched 2025-11-08 snapshot
  - MASTER.txt: 307,794 aircraft registrations (180 MB)
  - ACFTREF.txt: aircraft reference data (14 MB)
  - ENGINE.txt: engine reference data (227 KB)
- All tests passing (5/5)
- CLI working and tested

## Next steps
1. Build normalize pipeline
   - Parse MASTER.txt to aircraft + registrations + owners tables
   - Join ACFTREF and ENGINE reference tables
   - Apply lite address standardization
   - Write typed Parquet files with schema enforcement
2. Build publish pipeline (DuckDB + SQLite FTS)
3. Implement CLI query commands
4. Add verify checks and anomaly scans

