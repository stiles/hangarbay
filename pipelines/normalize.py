"""Normalize raw FAA files to typed Parquet tables."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
import xxhash
from rich.console import Console

# Global quiet flag
_quiet = False
from hangarbay.address import (
    clean_text,
    combine_address,
    standardize_owner_name,
    standardize_state,
    standardize_zip,
)
from hangarbay.schemas import (
    aircraft_schema,
    aircraft_make_model_schema,
    engines_schema,
    owners_schema,
    registrations_schema,
)

console = Console()


def generate_owner_id(row: dict) -> int:
    """
    Generate deterministic owner_id using xxhash64.
    
    Args:
        row: Dict with n_number and standardized owner fields
    
    Returns:
        64-bit hash as integer
    """
    # Create a stable string from key fields
    key_fields = [
        str(row.get("n_number", "")),
        str(row.get("owner_name_std", "")),
        str(row.get("address_all_std", "")),
        str(row.get("city_std", "")),
        str(row.get("state_std", "")),
        str(row.get("zip5", "")),
    ]
    
    key_string = "|".join(key_fields)
    return xxhash.xxh64(key_string.encode()).intdigest()


def parse_master_file(master_path: Path) -> tuple[pa.Table, pa.Table, pa.Table]:
    """
    Parse MASTER.txt into aircraft, registrations, and owners tables.
    
    Args:
        master_path: Path to MASTER.txt
    
    Returns:
        Tuple of (aircraft_table, registrations_table, owners_table)
    """
    if not _quiet: console.print(f"[cyan]Parsing {master_path.name}...[/cyan]")
    
    # Read the CSV with PyArrow
    # FAA files are comma-delimited with header row
    read_options = csv.ReadOptions(
        skip_rows=0,
        column_names=None,  # Use header row
    )
    
    parse_options = csv.ParseOptions(
        delimiter=",",
    )
    
    convert_options = csv.ConvertOptions(
        strings_can_be_null=True,
        null_values=["", "None"],
    )
    
    table = csv.read_csv(
        master_path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )
    
    if not _quiet: console.print(f"[green]✓ Read {len(table)} rows from MASTER.txt[/green]")
    
    # Convert to pandas for easier manipulation (we'll convert back to Arrow)
    df = table.to_pandas()
    
    # Clean up column names (strip whitespace)
    df.columns = df.columns.str.strip()
    
    # Build aircraft table (denormalized view with current registration facts)
    if not _quiet: console.print("[cyan]Building aircraft table...[/cyan]")
    aircraft_df = df[[
        "N-NUMBER", "SERIAL NUMBER", "MFR MDL CODE", "ENG MFR MDL",
        "YEAR MFR", "TYPE AIRCRAFT", 
    ]].copy()
    
    # Rename to match our schema
    aircraft_df.columns = [
        "n_number", "serial_no", "mfr_mdl_code", "engine_code",
        "year_mfr", "airworthiness_class",
    ]
    
    # Placeholder fields (we don't have these yet)
    aircraft_df["seats"] = None
    aircraft_df["engines"] = None
    
    # Add fields from registration
    aircraft_df["reg_status"] = df["STATUS CODE"].fillna("")
    
    # Parse dates from YYYYMMDD integer format
    import pandas as pd
    aircraft_df["status_date"] = pd.to_datetime(df["LAST ACTION DATE"], format="%Y%m%d", errors="coerce")
    aircraft_df["reg_expiration"] = pd.to_datetime(df["EXPIRATION DATE"], format="%Y%m%d", errors="coerce")
    
    aircraft_df["is_deregistered"] = False
    
    # Clean string fields
    aircraft_df["n_number"] = aircraft_df["n_number"].fillna("").str.strip()
    aircraft_df["serial_no"] = aircraft_df["serial_no"].fillna("").str.strip()
    aircraft_df["mfr_mdl_code"] = aircraft_df["mfr_mdl_code"].fillna("").str.strip()
    aircraft_df["engine_code"] = aircraft_df["engine_code"].fillna("").str.strip()
    aircraft_df["airworthiness_class"] = aircraft_df["airworthiness_class"].fillna("").str.strip()
    aircraft_df["reg_status"] = aircraft_df["reg_status"].fillna("").str.strip()
    
    # Clean numeric fields - convert to int, handle empty/invalid values
    import pandas as pd
    aircraft_df["year_mfr"] = pd.to_numeric(aircraft_df["year_mfr"], errors="coerce").astype("Int32")
    
    # Reorder columns to match schema
    aircraft_df = aircraft_df[[
        "n_number", "serial_no", "mfr_mdl_code", "engine_code", "year_mfr",
        "airworthiness_class", "seats", "engines", "reg_status", "status_date",
        "reg_expiration", "is_deregistered"
    ]]
    
    # Convert to Arrow and cast to schema
    aircraft_table = pa.Table.from_pandas(aircraft_df, preserve_index=False)
    aircraft_table = aircraft_table.cast(aircraft_schema, safe=False)
    
    if not _quiet: console.print(f"[green]✓ Built aircraft table: {len(aircraft_table)} rows[/green]")
    
    # Build registrations table (canonical registration state)
    if not _quiet: console.print("[cyan]Building registrations table...[/cyan]")
    registrations_df = df[[
        "N-NUMBER", "CERTIFICATION", "STATUS CODE", 
        "LAST ACTION DATE", "EXPIRATION DATE"
    ]].copy()
    
    registrations_df.columns = [
        "n_number", "reg_type", "reg_status", "status_date", "reg_expiration"
    ]
    
    registrations_df["n_number"] = registrations_df["n_number"].fillna("").str.strip()
    registrations_df["reg_type"] = registrations_df["reg_type"].fillna("").str.strip()
    registrations_df["reg_status"] = registrations_df["reg_status"].fillna("").str.strip()
    
    # Parse dates from YYYYMMDD format
    registrations_df["status_date"] = pd.to_datetime(registrations_df["status_date"], format="%Y%m%d", errors="coerce")
    registrations_df["reg_expiration"] = pd.to_datetime(registrations_df["reg_expiration"], format="%Y%m%d", errors="coerce")
    
    registrations_table = pa.Table.from_pandas(registrations_df, preserve_index=False)
    registrations_table = registrations_table.cast(registrations_schema, safe=False)
    
    if not _quiet: console.print(f"[green]✓ Built registrations table: {len(registrations_table)} rows[/green]")
    
    # Build owners table (with address standardization)
    if not _quiet: console.print("[cyan]Building owners table...[/cyan]")
    
    owners_list = []
    for idx, row in df.iterrows():
        n_number = str(row.get("N-NUMBER", "")).strip()
        
        # Raw fields
        owner_name_raw = str(row.get("NAME", "")).strip()
        address1_raw = str(row.get("STREET", "")).strip()
        address2_raw = str(row.get("STREET2", "")).strip()
        city_raw = str(row.get("CITY", "")).strip()
        state_raw = str(row.get("STATE", "")).strip()
        zip_raw = str(row.get("ZIP CODE", "")).strip()
        owner_type = str(row.get("TYPE REGISTRANT", "")).strip()
        
        # Standardized fields
        owner_name_std = standardize_owner_name(owner_name_raw)
        address_all_std = combine_address(address1_raw, address2_raw)
        city_std = clean_text(city_raw)
        state_std = standardize_state(state_raw)
        zip5 = standardize_zip(zip_raw)
        
        # Generate deterministic owner_id
        owner_row = {
            "n_number": n_number,
            "owner_name_std": owner_name_std,
            "address_all_std": address_all_std,
            "city_std": city_std,
            "state_std": state_std,
            "zip5": zip5,
        }
        owner_id = generate_owner_id(owner_row)
        
        owners_list.append({
            "owner_id": owner_id,
            "n_number": n_number,
            "owner_type": owner_type,
            "owner_name_raw": owner_name_raw,
            "address1_raw": address1_raw,
            "address2_raw": address2_raw,
            "city_raw": city_raw,
            "state_raw": state_raw,
            "zip_raw": zip_raw,
            "owner_name_std": owner_name_std,
            "address_all_std": address_all_std,
            "city_std": city_std,
            "state_std": state_std,
            "zip5": zip5,
        })
    
    import pandas as pd
    owners_df = pd.DataFrame(owners_list)
    owners_table = pa.Table.from_pandas(owners_df, preserve_index=False)
    owners_table = owners_table.cast(owners_schema, safe=False)
    
    if not _quiet: console.print(f"[green]✓ Built owners table: {len(owners_table)} rows[/green]")
    
    return aircraft_table, registrations_table, owners_table


def parse_acftref_file(acftref_path: Path) -> pa.Table:
    """
    Parse ACFTREF.txt into aircraft_make_model table.
    
    Args:
        acftref_path: Path to ACFTREF.txt
    
    Returns:
        PyArrow table
    """
    if not _quiet: console.print(f"[cyan]Parsing {acftref_path.name}...[/cyan]")
    
    read_options = csv.ReadOptions(skip_rows=0, column_names=None)
    parse_options = csv.ParseOptions(delimiter=",")
    convert_options = csv.ConvertOptions(strings_can_be_null=True, null_values=["", "None"])
    
    table = csv.read_csv(
        acftref_path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )
    
    df = table.to_pandas()
    df.columns = df.columns.str.strip()
    
    # Map to our schema
    # FAA columns: CODE, MFR, MODEL, TYPE-ACFT, TYPE-ENG, AC-CAT, BUILD-CERT-IND, NO-ENG, NO-SEATS, AC-WEIGHT, SPEED
    acft_df = df[[
        "CODE", "MFR", "MODEL", "AC-CAT", "TYPE-ACFT", "TYPE-ENG", "NO-SEATS"
    ]].copy()
    
    acft_df.columns = ["mfr_mdl_code", "maker", "model", "category", "type", "engine_type", "seats_default"]
    
    # Clean strings - convert to string first to handle any numeric values
    for col in ["mfr_mdl_code", "maker", "model", "category", "type", "engine_type"]:
        acft_df[col] = acft_df[col].astype(str).str.strip().replace("nan", "")
    
    acft_table = pa.Table.from_pandas(acft_df, preserve_index=False)
    acft_table = acft_table.cast(aircraft_make_model_schema, safe=False)
    
    if not _quiet: console.print(f"[green]✓ Built aircraft_make_model table: {len(acft_table)} rows[/green]")
    
    return acft_table


def parse_engine_file(engine_path: Path) -> pa.Table:
    """
    Parse ENGINE.txt into engines table.
    
    Args:
        engine_path: Path to ENGINE.txt
    
    Returns:
        PyArrow table
    """
    if not _quiet: console.print(f"[cyan]Parsing {engine_path.name}...[/cyan]")
    
    read_options = csv.ReadOptions(skip_rows=0, column_names=None)
    parse_options = csv.ParseOptions(delimiter=",")
    convert_options = csv.ConvertOptions(strings_can_be_null=True, null_values=["", "None"])
    
    table = csv.read_csv(
        engine_path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )
    
    df = table.to_pandas()
    df.columns = df.columns.str.strip()
    
    # Map to our schema
    # FAA columns: CODE, MFR, MODEL, TYPE, HORSEPOWER, THRUST
    engine_df = df[["CODE", "MFR", "MODEL", "TYPE", "HORSEPOWER"]].copy()
    engine_df.columns = ["engine_code", "manufacturer", "model", "type", "horsepower"]
    
    # Add cylinders (not in FAA data, set to null)
    engine_df["cylinders"] = None
    
    # Clean strings - convert to string first to handle any numeric values
    for col in ["engine_code", "manufacturer", "model", "type"]:
        engine_df[col] = engine_df[col].astype(str).str.strip().replace("nan", "")
    
    engine_table = pa.Table.from_pandas(engine_df, preserve_index=False)
    engine_table = engine_table.cast(engines_schema, safe=False)
    
    if not _quiet: console.print(f"[green]✓ Built engines table: {len(engine_table)} rows[/green]")
    
    return engine_table


def normalize(
    data_root: Path = Path("data"),
    snapshot_date: Optional[str] = None,
    quiet: bool = False,
) -> Path:
    """
    Normalize raw FAA files to typed Parquet tables.
    
    Args:
        data_root: Root data directory
        snapshot_date: Snapshot date (YYYY-MM-DD), defaults to latest
    
    Returns:
        Path to publish directory
    """
    global _quiet
    _quiet = quiet
    if snapshot_date is None:
        # Find the latest snapshot
        raw_dir = data_root / "raw"
        if not raw_dir.exists():
            if not _quiet: console.print("[red]No raw data found. Run 'hangar fetch' first.[/red]")
            raise FileNotFoundError("No raw data directory")
        
        snapshots = sorted([d.name for d in raw_dir.iterdir() if d.is_dir()])
        if not snapshots:
            if not _quiet: console.print("[red]No snapshots found. Run 'hangar fetch' first.[/red]")
            raise FileNotFoundError("No snapshots found")
        
        snapshot_date = snapshots[-1]
    
    raw_snapshot = data_root / "raw" / snapshot_date
    if not raw_snapshot.exists():
        if not _quiet: console.print(f"[red]Snapshot {snapshot_date} not found[/red]")
        raise FileNotFoundError(f"Snapshot directory not found: {raw_snapshot}")
    
    if not _quiet: console.print(f"\n[bold cyan]Normalizing snapshot: {snapshot_date}[/bold cyan]\n")
    
    # Paths
    master_path = raw_snapshot / "MASTER.txt"
    acftref_path = raw_snapshot / "ACFTREF.txt"
    engine_path = raw_snapshot / "ENGINE.txt"
    
    # Create publish directory
    publish_dir = data_root / "publish"
    publish_dir.mkdir(parents=True, exist_ok=True)
    
    # Parse all files
    aircraft_table, registrations_table, owners_table = parse_master_file(master_path)
    acft_ref_table = parse_acftref_file(acftref_path)
    engines_table = parse_engine_file(engine_path)
    
    # Write Parquet files
    if not _quiet: console.print(f"\n[cyan]Writing Parquet files to {publish_dir}...[/cyan]")
    
    import pyarrow.parquet as pq
    
    pq.write_table(aircraft_table, publish_dir / "aircraft.parquet")
    if not _quiet: console.print("[green]✓ Wrote aircraft.parquet[/green]")
    
    pq.write_table(registrations_table, publish_dir / "registrations.parquet")
    if not _quiet: console.print("[green]✓ Wrote registrations.parquet[/green]")
    
    pq.write_table(owners_table, publish_dir / "owners.parquet")
    if not _quiet: console.print("[green]✓ Wrote owners.parquet[/green]")
    
    pq.write_table(acft_ref_table, publish_dir / "aircraft_make_model.parquet")
    if not _quiet: console.print("[green]✓ Wrote aircraft_make_model.parquet[/green]")
    
    pq.write_table(engines_table, publish_dir / "engines.parquet")
    if not _quiet: console.print("[green]✓ Wrote engines.parquet[/green]")
    
    # Write metadata
    metadata = {
        "snapshot_date": snapshot_date,
        "normalized_at": datetime.utcnow().isoformat() + "Z",
        "row_counts": {
            "aircraft": len(aircraft_table),
            "registrations": len(registrations_table),
            "owners": len(owners_table),
            "aircraft_make_model": len(acft_ref_table),
            "engines": len(engines_table),
        },
    }
    
    meta_dir = publish_dir / "_meta"
    meta_dir.mkdir(exist_ok=True)
    
    with open(meta_dir / "normalize.json", "w") as f:
        json.dump(metadata, f, indent=2)
    
    if not _quiet: console.print(f"[green]✓ Wrote metadata to {meta_dir / 'normalize.json'}[/green]")
    
    if not _quiet: console.print(f"\n[bold green]✓ Normalization complete![/bold green]")
    if not _quiet: console.print(f"[dim]Published to: {publish_dir}[/dim]\n")
    
    return publish_dir


if __name__ == "__main__":
    normalize()
