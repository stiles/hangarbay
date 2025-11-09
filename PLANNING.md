# FAA registry workflow plan (hangarbay)

## Overview
A reproducible workflow for the FAA aircraft registry you can pick up after months away. Clean ingest → normalize → publish → query. Keep provenance. Fast N-number lookups, deeper joins when needed.

## What changed since v2
- Clarified how **DEREG** flows into normalized tables
- Relaxed verify rule on manufacture year
- Noted N-number validation nuance and future strict mode
- Added a note on PyPI Trusted Publisher setup time
- Clarified physical storage for reference tables
- Decided the shape of `owners_summary`
- Listed additional anomaly scans

## Implementation status (as of 2025-11-09)

**✅ MVP COMPLETE (v0.3.0):**
- Project scaffolding (pyproject.toml, Makefile, directory structure)
- Arrow schemas with provenance (hangarbay/schemas.py)
- Fetch pipeline with browser headers to avoid FAA blocking
- Manifest generation with SHA256 hashes
- Normalize pipeline: MASTER → aircraft + registrations + owners, ACFTREF, ENGINE
- Address standardization utilities
- Publish pipeline: DuckDB (106 MB) + SQLite FTS (55 MB)
- CLI foundation with Typer (hangar command)
- `hangar search` - N-number lookup with decoded status/certificate codes
- `hangar sql` - Execute queries with table/json/csv output and case-insensitive flag
- Test suite (10/10 passing)
- Successfully processing 307,793 aircraft registrations

**📋 FUTURE ENHANCEMENTS:**
- Better CLI output of tables (both raw and standard columns appear)
  - Codes such as "owner_type" aren't explained
- Python API for programmatic access (`from hangarbay import Registry`)
- `hangar fleet` command for owner-based fleet search
- Verify checks and anomaly scans (soft warnings)
- Historical diffs across snapshots
- DEREG integration for deregistration tracking
- CI/CD setup with GitHub Actions
- PyPI publishing

---

## Data sources and cadence

### Files to fetch (minimum viable set)
- **MASTER (Aircraft Registration Master)** — primary registration facts
- **ACFTREF** — aircraft make/model reference
- **ENGINE** — engine reference

### Optional later
- **DEREG** — deregistered tails for status history and diffs
- **DEALER** — dealer certificates
- **DOCINDEX** — document index for deep audit trails

### Cadence
- **On-demand** (manual `make all`) for investigations
- **Scheduled** monthly via GitHub Actions or cron
- Keep the last 12 snapshots. Always retain “previous” for diffing

### File size handling
- Stream downloads and record SHA256 in `manifest.json`
- Parse with PyArrow. Use streaming reads or per-table splits if needed
- No hard-coded row counts. Verify counts against the manifest

---

## Schema and tables

### aircraft (current facts about the airframe)
- `n_number` (PK, text), `serial_no`, `mfr_mdl_code`, `engine_code`, `year_mfr`, `airworthiness_class`, `seats`, `engines`
- Convenience mirror of current registration flags: `reg_status`, `status_date`, `reg_expiration`
- Represents the **latest** values for quick lookups

### registrations (normalized registration state)
- `n_number` (PK), `reg_type`, `reg_status`, `status_date`, `reg_expiration`
- Authoritative registration attributes as they appear in the snapshot
- Why both tables? `aircraft` is the denormalized quick view used in most lookups. `registrations` is the canonical slice for joins and diffs. We validate shared fields for equality on publish

### owners (multi-row per N-number supported)
- `owner_id` (PK), `n_number` (FK), `owner_type`, raw address fields
- Standardized adds: `owner_name_std`, `address_all_std`, `city_std`, `state_std`, `zip5`

### aircraft_make_model
- `mfr_mdl_code` (PK) → `maker`, `model`, `category`, `type`, `engine_type`, `seats_default`

### engines
- `engine_code` (PK) → `manufacturer`, `model`, `type`, `horsepower`, `cylinders`

### owner_id strategy
- Deterministic **hash-based** ID on ingest to avoid churn across runs:  
  `owner_id = xxhash64(n_number, owner_name_std, address_all_std, city_std, state_std, zip5)`  
- Tolerates ordering differences. If a co-owner line changes text the hash changes and we can diff it

### Multi-owner handling
- Keep **one row per owner-party per n_number** in `owners`
- `owners_summary` materialized as a Parquet table for convenience with columns:
  - `n_number`, `owner_count`, `owner_names_concat` (semicolon-joined `owner_name_std`), `any_trust_flag`
- Also create a cheap DuckDB view with the same logic so ad-hoc SQL stays simple

---

## How (optional, later) DEREG integrates
- Store the raw **DEREG** file under the snapshot date like other sources
- Normalize to a small table `deregistrations`:
  - `n_number` (PK), `dereg_date`, `reason_code`, `new_mark` (if present), `notes_raw`
- On publish:
  - Left-join **deregistrations** into `registrations` to enrich `reg_status` where appropriate
  - Add a convenience boolean `is_deregistered` on `aircraft` based on a join hit
- We keep `deregistrations` separate so history stays queryable and easy to diff across snapshots

---

## Reference table versioning and storage
- Reference tables (ACFTREF, ENGINE) are copied into `data/raw/YYYY-MM-DD/` for provenance
- In `lookups/` we keep **normalized Parquet** versions keyed by the same snapshot date (no symlinks)
- If a reference file changes without MASTER changing, we still stamp a new snapshot and rebuild normalized lookups

---

## Address standardization (lite)
- Uppercase, trim, collapse whitespace
- `address_all_std = join(address1_raw, address2_raw)`
- Normalize USPS state to 2-letter
- `zip5 = leftpad(digits(zip_raw), 5)`
- Keep raw and standardized fields. No geocoding in MVP

---

## Avoiding type drift
- Arrow schemas live in `hangarbay/schemas.py`
- Cast all inputs to declared schemas before write
- Manifest stores a `schema_hash` per table for auditing

**Example sketch**
```python
# hangarbay/schemas.py
import pyarrow as pa

aircraft_schema = pa.schema([
    ("n_number", pa.string()),
    ("serial_no", pa.string()),
    ("mfr_mdl_code", pa.string()),
    ("engine_code", pa.string()),
    ("year_mfr", pa.int32()),
    ("airworthiness_class", pa.string()),
    ("seats", pa.int32()),
    ("engines", pa.int32()),
    ("reg_status", pa.string()),
    ("status_date", pa.date32()),
    ("reg_expiration", pa.date32()),
    ("is_deregistered", pa.bool_()),
])
```

---

## Validation and anomaly scans

### Verify checks (hard or soft)
- **Unique**: no duplicate `n_number` in `aircraft` (hard)
- **Year bounds (soft)**: `year_mfr <= current_year + 2` (warn if violated)
- **Active implies future**: if `reg_status = 'Active'` then `reg_expiration >= today()` (warn)
- **Format (loose)**: `n_number` matches `^N[0-9A-Z]{1,5}$` (warn only; FAA rules are more nuanced and forbid leading zeros after N)
- **ZIP sanity**: `zip5` is 5 digits for US states (warn)
- **Coverage**: ≥ 98% of `mfr_mdl_code` and `engine_code` resolve (warn)

### Additional anomaly scans (soft warnings)
- Very old airframes still active (e.g., `year_mfr < 1920`)
- Duplicate serial numbers across different `n_number` in same make/model
- Owner names that look obviously malformed (length 1, high digit share)
- Sudden row-count deltas vs previous snapshot beyond a threshold

---

## Performance notes
- PyArrow CSV is fast enough for 300k+ rows. Profile later if needed
- DuckDB over partitioned Parquet is instant for joins and aggregations
- SQLite FTS5 index build on `owners` may take about a minute on first publish then sub-second searches

---

## Testing strategy
- **CI sample**: curated fixture (~200–500 rows per table) in `tests/fixtures/`
- Unit tests: schema casting, address standardization, owner_id hashing, FTS creation
- Pipeline test: run `normalize` and `publish` against fixtures on CI
- Local full-run tests with real downloads outside CI

---

## Error handling and retries
- `fetch`: retry with backoff, verify SHA256, fail clearly on corruption
- `normalize`: fail fast on cast errors, write row-level error report when possible
- `publish`: atomic writes (temp then move). Previous publish remains intact on failure
- Optional Slack hook for scheduled failures

---

## Interfaces

### CLI (Typer)
- `hangar search N12345` — pretty-print a single registration
- `hangar owners --name "NETJETS" --state OH` — party list with counts
- `hangar fleet --contains "AMAZON" --export fleet.parquet`
- `hangar sql "<query>"` — **read-only** by default; `--write` required to enable DDL in DuckDB

### Python API
```python
from hangarbay import Registry
reg = Registry(path="data/publish/")
reg.lookup_n("N12345")            # dict or DataFrame
reg.fleet("NETJETS")              # DataFrame
reg.duckdb(read_only=True).sql("select count(*) from aircraft").df()
```

---

## Update strategy
Use **make** as the interface. Idempotent by hash.

```makefile
fetch:        ## Download latest raw files, SHA256, manifest.json
normalize:    ## Parse to Arrow, cast to schemas, join refs, lite address cleanup
publish:      ## Parquet + DuckDB + owners.sqlite (FTS) + _meta
verify:       ## Row counts, schema checks, anomaly scans
all: fetch normalize publish verify
```

---

## Packaging and distribution
- Python package `hangarbay` on PyPI with Trusted Publisher
- CLI command: `hangar`
- Trusted Publisher setup is quick but budget ~1 hour if first time (OIDC + PyPI org)
- Single-file DuckDB + Parquet artifacts for newsroom sharing
- Optional Docker for hermetic runs if teammates lack compilers
- License: MIT

---

## Folder layout
```
hangarbay/
  Makefile
  pyproject.toml
  hangarbay/              # package (cli + api + schemas)
  pipelines/              # fetch, normalize, publish
  lookups/                # normalized Parquet copies per snapshot date
  tests/                  # fixtures + unit tests
  data/
    raw/2025-11-08/
    interim/
    publish/              # *.parquet, registry.duckdb, owners.sqlite, _meta/
  docs/
```

## MVP checklist

**✅ Completed:**
- Fetch + manifest with schema_hash
- Parse to Parquet with Arrow schemas and lite address cleanup
- Build registry.duckdb + owners.sqlite (FTS)
- Materialize `owners_summary`
- CLI: search and sql (read-only)
- Unit tests for schemas, address standardization and owner_id

**📋 Remaining (Future):**
- Integrate DEREG into `registrations` and `aircraft.is_deregistered`
- CLI: owners and fleet commands
- Verify: checks + anomaly scans
- CI fixture for automated testing

---

## Philosophy
This is a researcher’s tool first, a shareable kit second. The architecture supports both without overbuilding. Provenance stays cheap. Queries stay fast. No mystery glue.
