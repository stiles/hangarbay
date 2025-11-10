# Hangarbay

**Stop struggling with FAA registration data.** Get clean, queryable aircraft registry tables in seconds.

Hangarbay downloads, normalizes and indexes the complete [FAA aircraft registry](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download) so you can skip straight to analysis. No more hunting for files to download, or joining fragmented tables, or decoding cryptic lookup relationships. One command gets you 307,000+ aircraft registrations, owners and specifications ready to query with SQL or pandas.

This is a tool built for researchers, data journalists and aviation analysts who need fast, reliable and repeatable workflows. 

## Quick start

**Requirements:** Python 3.9+

### Python API (notebooks & scripts)

```python
# Install
pip install hangarbay

# One-time setup
import hangarbay as hb
hb.load_data()  # Downloads & processes FAA data to ~/.hangarbay/data/

# Look up aircraft
df = hb.search("N221LA")

# Find fleets
fleet = hb.fleet("United Airlines")
fleet = hb.fleet("LAPD|Los Angeles Police", state="CA")

# Custom SQL
df = hb.query("""
    SELECT maker, COUNT(*) as count
    FROM aircraft_decoded
    WHERE year_mfr > 2020
    GROUP BY maker
    ORDER BY count DESC
""")

# Check data age
info = hb.status()
print(f"Data is {info['age_days']} days old")
```

### Command-line interface

```bash
# Install
pip install hangarbay

# Run the full pipeline (< 1 minute, depending on your connection)
hangar update     # Download, normalize and publish (in one step)

# Or run steps individually
hangar fetch      # Download FAA data
hangar normalize  # Parse to typed Parquet tables
hangar publish    # Build DuckDB + SQLite FTS indexes

# Check data status and age
hangar status

# Start querying!
hangar search N757AF
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
- **Shared data directory** at `~/.hangarbay/data/` - works from any project or notebook

## Features

### N-Number lookup
Look up any aircraft registration with decoded status codes and human-readable output:

```bash
hangar search N221LA
```

### Fleet search
Find all aircraft owned by a person or company:

```bash
# Search by owner name (case-insensitive)
hangar fleet "United Airlines"
hangar fleet "LAPD"
hangar fleet "NetJets"

# OR logic with pipe separator (great for finding multiple airlines)
hangar fleet "LAPD|Los Angeles Police"     # Either pattern matches
hangar fleet "Delta|American|United"       # Any of the three

# Filter by state
hangar fleet "Delta" --state GA

# Export to CSV
hangar fleet "Boeing" --export boeing_fleet.csv

# Limit results
hangar fleet "Cessna" --limit 20
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

# Use decoded views for readable output
hangar sql "SELECT * FROM aircraft_decoded WHERE year_mfr > 2020 LIMIT 10"
hangar sql "SELECT * FROM owners_clean WHERE owner_name LIKE '%boeing%'" -i

# Top manufacturers
hangar sql "SELECT maker, COUNT(*) as count FROM aircraft 
  JOIN aircraft_make_model USING(mfr_mdl_code) 
  WHERE maker != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 10"

# Lookup status codes
hangar sql "SELECT * FROM status_codes"

# Top states by registrations
hangar sql "SELECT state_std, COUNT(*) as count FROM owners 
  WHERE state_std != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
```

**Output Formats:**

```bash
# Pretty table (default)
hangar sql "SELECT * FROM status_codes LIMIT 3"

# JSON output (for APIs/scripts)
hangar sql "SELECT * FROM status_codes LIMIT 3" --output-format json

# CSV output (for Excel/spreadsheets)
hangar sql "SELECT * FROM status_codes LIMIT 3" --output-format csv
```

### Available tables

**Core Tables:**
- **aircraft** - Current registration facts (N-number, serial, make/model, year, status)
- **registrations** - Canonical registration state (type, status, dates)
- **owners** - Owner records with raw and standardized addresses
- **owners_summary** - Aggregated ownership (co-owners, counts)
- **aircraft_make_model** - Make/model reference (Cessna 172, Boeing 737, etc.)
- **engines** - Engine specifications (manufacturer, horsepower, type)

**Decoded Views (Recommended):**
- **aircraft_decoded** - Aircraft with decoded status codes and joined make/model
- **owners_clean** - Simplified owner table (standardized fields only, decoded owner type)

**Reference Tables:**
- **status_codes** - Registration status code lookups
- **airworthiness_classes** - Airworthiness certificate class lookups  
- **owner_types** - Owner type code lookups

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

To contribute or modify the code:

```bash
# Clone and install for development
git clone https://github.com/stiles/hangarbay.git
cd hangarbay
pip install -e ".[dev]"

# Run tests
make test

# Update data
make update       # or: hangar update

# Check data status
make status       # or: hangar status

# Full pipeline (individual steps)
make all          # fetch, normalize, publish, verify

# Individual steps
make fetch
make normalize
make publish

# Clean intermediate files
make clean
```

## License

MIT

