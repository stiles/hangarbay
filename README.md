# Hangarbay

A reproducible workflow for the [FAA aircraft registry](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download). Download raw data, normalize it into typed tables and query with SQL. Fast lookups for individual aircraft, powerful queries for fleet analysis.

## Quick start

**Requirements:** Python 3.9+

```bash
# Clone and install
git clone https://github.com/stiles/hangarbay.git
cd hangarbay
pip install -e ".[dev]"

# Run the full pipeline (~2 minutes)
hangar fetch      # Download FAA data
hangar normalize  # Parse to typed Parquet tables
hangar publish    # Build DuckDB + SQLite FTS indexes

# Or use make
make all

# Start querying!
hangar search N100
hangar sql "SELECT COUNT(*) FROM aircraft"
```

## What this does

Hangarbay downloads FAA aircraft registration data, normalizes it into typed tables and provides fast SQL querying:

- **307K+ aircraft registrations** with owner and address information
- **93K+ make/model references** (Cessna, Piper, Boeing, etc.)
- **4,700+ engine specifications** (horsepower, type, manufacturer)
- **DuckDB** for analytical SQL queries (sub-second on 300K+ rows)
- **SQLite FTS5** for full-text search by owner name or address
- **Parquet** files for efficient columnar storage
- **Data lineage tracking** with SHA256 checksums and version metadata

## Features

### N-Number Lookup
Look up any aircraft registration with decoded status codes and human-readable output:

```bash
hangar search N221LA
```

```
Aircraft Registration: N221LA

Owner: LAPD AIR SUPPORT DIVISION
Location: LOS ANGELES, CA

  Make & Model:           AIRBUS HELICOPTERS INC AS350B3
  Year Manufactured:      2014
  Serial Number:          7900
  Registration Status:    Valid
  Certificate Type:       Standard Airworthiness - Normal
  Status Date:            May 19, 2023
  Expiration:             Jun 30, 2028
```

### SQL queries
Execute analytical queries with pretty output:

```bash
# Total aircraft
hangar sql "SELECT COUNT(*) FROM aircraft"

# Top manufacturers
hangar sql "SELECT maker, COUNT(*) as count FROM aircraft 
  JOIN aircraft_make_model USING(mfr_mdl_code) 
  WHERE maker != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 10"

# Find company fleets (case-insensitive search)
hangar sql "SELECT * FROM owners WHERE owner_name_std LIKE '%boeing%'" -i

# Top states by registrations
hangar sql "SELECT state_std, COUNT(*) as count FROM owners 
  WHERE state_std != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 10"

# Export to JSON or CSV
hangar sql "SELECT * FROM engines LIMIT 5" --output-format json
hangar sql "SELECT * FROM engines LIMIT 5" --output-format csv
```

### Available tables
- **aircraft** - Current registration facts (N-number, serial, make/model, year, status)
- **registrations** - Canonical registration state (type, status, dates)
- **owners** - Owner records with raw and standardized addresses
- **owners_summary** - Aggregated ownership (co-owners, counts)
- **aircraft_make_model** - Make/model reference (Cessna 172, Boeing 737, etc.)
- **engines** - Engine specifications (manufacturer, horsepower, type)

## Data quality

- **Type safety**: PyArrow schemas enforce correct data types (dates, integers, strings)
- **Address standardization**: Uppercase, state/ZIP normalization, whitespace cleanup
- **Deterministic IDs**: xxhash64 ensures consistent owner IDs across pipeline runs
- **File verification**: SHA256 checksums validate all downloads
- **Version tracking**: Manifest records schema versions and row counts for each snapshot

## Architecture

```
FAA Data (MASTER, ACFTREF, ENGINE)
    ↓ fetch (with SHA256 verification)
Raw Text Files (275 MB)
    ↓ normalize (PyArrow type casting)
Typed Parquet Tables (36 MB)
    ↓ publish (indexing)
DuckDB (106 MB) + SQLite FTS (55 MB)
    ↓ query
CLI / Python API
```

## Development

```bash
# Run tests
make test

# Full pipeline
make all

# Individual steps
make fetch
make normalize
make publish

# Clean intermediate files
make clean
```

## Design philosophy

Built for researchers and data journalists who need reliable, repeatable analysis. The architecture is simple: download → parse → index → query. No black boxes or vendors. Transparent data flow, efficient storage and fast queries. 

## License

MIT

