"""Public Python API for hangarbay.

This module provides a simple interface for working with FAA aircraft registry data
in Python notebooks and scripts.

Example usage:
    >>> import hangarbay as hb
    >>> hb.load_data()
    >>> df = hb.search("N221LA")
    >>> fleet = hb.fleet("United Airlines")
"""
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
from rich.console import Console

from hangarbay import config
from pipelines import fetch, normalize, publish

console = Console()


@contextmanager
def _suppress_output():
    """Temporarily suppress stdout and stderr for quiet mode."""
    import os
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    
    # Also suppress file descriptor output (for Rich/console libraries)
    old_stdout_fd = os.dup(1)
    old_stderr_fd = os.dup(2)
    devnull = os.open(os.devnull, os.O_WRONLY)
    
    try:
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        os.dup2(devnull, 1)
        os.dup2(devnull, 2)
        yield
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        os.dup2(old_stdout_fd, 1)
        os.dup2(old_stderr_fd, 2)
        os.close(devnull)
        os.close(old_stdout_fd)
        os.close(old_stderr_fd)


def _check_data_exists() -> bool:
    """Check if data has been downloaded and processed."""
    data_dir = config.get_data_dir()
    duckdb_path = data_dir / "publish" / "registry.duckdb"
    return duckdb_path.exists()


def _get_data_age_days() -> Optional[int]:
    """Get the age of the current data in days."""
    data_dir = config.get_data_dir()
    normalize_meta = data_dir / "publish" / "_meta" / "normalize.json"
    
    if not normalize_meta.exists():
        return None
    
    with open(normalize_meta) as f:
        meta = json.load(f)
    
    snapshot_date = datetime.fromisoformat(meta["snapshot_date"])
    age = datetime.now() - snapshot_date
    return age.days


def _warn_if_stale(skip_age_check: bool = False):
    """Warn user if data is stale."""
    if skip_age_check:
        return
    
    age_days = _get_data_age_days()
    if age_days and age_days > 30:
        console.print(
            f"\n[yellow]⚠ Data is {age_days} days old. "
            f"Consider running hb.load_data() to update.[/yellow]\n"
        )


def _ensure_data():
    """Ensure data exists, download if necessary."""
    if not _check_data_exists():
        console.print(
            "\n[yellow]⚠ No data found. Downloading FAA registry data...[/yellow]"
        )
        console.print("[dim]This is a one-time download (~400MB, may take a few minutes)[/dim]\n")
        load_data(force=True, skip_age_check=True)


def load_data(force: bool = False, skip_age_check: bool = False, quiet: bool = False) -> None:
    """Download and process FAA aircraft registry data.
    
    This function runs the complete pipeline: fetch -> normalize -> publish.
    Data is stored in ~/.hangarbay/data/ by default.
    
    Args:
        force: If True, re-download even if data exists
        skip_age_check: If True, skip warning about stale data
        quiet: If True, suppress progress output (useful for notebooks)
    
    Example:
        >>> import hangarbay as hb
        >>> hb.load_data()
        >>> hb.load_data(quiet=True)  # For notebooks
    """
    data_dir = config.ensure_data_dir()
    
    # Check if data exists and is recent
    if not force and _check_data_exists():
        age_days = _get_data_age_days()
        if age_days and age_days <= 30:
            if not skip_age_check and not quiet:
                console.print(f"[green]✓ Data is up-to-date ({age_days} days old)[/green]")
            return
        elif not skip_age_check and not quiet:
            console.print(f"[yellow]Data is {age_days} days old. Updating...[/yellow]")
    
    # Run the pipeline
    if not quiet:
        console.print(f"[cyan]Installing data to: {data_dir}[/cyan]\n")
        console.print("[bold]Step 1/3:[/bold] Fetching data from FAA...")
    
    fetch.fetch(data_dir, quiet=quiet)
    
    if not quiet:
        console.print("\n[bold]Step 2/3:[/bold] Normalizing and cleaning...")
    
    normalize.normalize(data_dir, quiet=quiet)
    
    if not quiet:
        console.print("\n[bold]Step 3/3:[/bold] Building databases...")
    
    publish.publish(data_dir, quiet=quiet)
    
    if not quiet:
        console.print("\n[green]✓ Setup complete! Ready to query.[/green]")


def search(n_number: str, skip_age_check: bool = False) -> pd.DataFrame:
    """Look up an aircraft by N-number.
    
    Args:
        n_number: Aircraft registration number (with or without "N" prefix)
        skip_age_check: Skip data age warning
    
    Returns:
        DataFrame with aircraft and owner information
    
    Example:
        >>> import hangarbay as hb
        >>> df = hb.search("N221LA")
        >>> df = hb.search("221LA")  # Works without "N" too
    """
    _ensure_data()
    _warn_if_stale(skip_age_check)
    
    # Normalize N-number
    n_number = n_number.upper().strip()
    if n_number.startswith("N"):
        n_number = n_number[1:]
    
    data_dir = config.get_data_dir()
    db_path = data_dir / "publish" / "registry.duckdb"
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    query = """
        SELECT 
            a.*,
            o.owner_name,
            o.city,
            o.state,
            o.zip,
            o.address
        FROM aircraft_decoded a
        LEFT JOIN owners_clean o ON a.n_number = o.n_number
        WHERE UPPER(a.n_number) = UPPER(?)
    """
    
    df = conn.execute(query, [n_number]).fetchdf()
    conn.close()
    
    return df


def fleet(
    owner: str,
    state: Optional[str] = None,
    limit: int = 0,
    skip_age_check: bool = False
) -> pd.DataFrame:
    """Find all aircraft owned by a person or company.
    
    Args:
        owner: Owner name to search (case-insensitive, use | for OR logic)
        state: Optional state filter (e.g., "CA", "TX")
        limit: Maximum number of results (0 for unlimited)
        skip_age_check: Skip data age warning
    
    Returns:
        DataFrame with aircraft fleet information
    
    Example:
        >>> import hangarbay as hb
        >>> df = hb.fleet("United Airlines")
        >>> df = hb.fleet("LAPD|Los Angeles Police", state="CA")
        >>> df = hb.fleet("Delta", limit=10)
    """
    _ensure_data()
    _warn_if_stale(skip_age_check)
    
    data_dir = config.get_data_dir()
    db_path = data_dir / "publish" / "registry.duckdb"
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    # Split on pipe for OR logic
    search_terms = [term.strip() for term in owner.split('|')]
    
    # Build query
    query = """
        SELECT 
            a.n_number,
            a.maker,
            a.model,
            a.year_mfr,
            a.reg_status,
            o.owner_name,
            o.city,
            o.state
        FROM aircraft_decoded a
        JOIN owners_clean o ON a.n_number = o.n_number
        WHERE (
    """
    
    like_conditions = []
    params = []
    for term in search_terms:
        like_conditions.append("LOWER(o.owner_name) LIKE LOWER(?)")
        params.append(f"%{term}%")
    
    query += " OR ".join(like_conditions)
    query += ")"
    
    if state:
        query += " AND UPPER(o.state) = UPPER(?)"
        params.append(state)
    
    query += " ORDER BY a.n_number"
    
    if limit > 0:
        query += f" LIMIT {limit}"
    
    df = conn.execute(query, params).fetchdf()
    conn.close()
    
    return df


def query(sql: str, skip_age_check: bool = False) -> pd.DataFrame:
    """Execute a custom SQL query against the aircraft database.
    
    Args:
        sql: SQL query string
        skip_age_check: Skip data age warning
    
    Returns:
        DataFrame with query results
    
    Example:
        >>> import hangarbay as hb
        >>> df = hb.query('''
        ...     SELECT maker, COUNT(*) as count
        ...     FROM aircraft_decoded
        ...     WHERE year_mfr > 2020
        ...     GROUP BY maker
        ...     ORDER BY count DESC
        ...     LIMIT 10
        ... ''')
    """
    _ensure_data()
    _warn_if_stale(skip_age_check)
    
    data_dir = config.get_data_dir()
    db_path = data_dir / "publish" / "registry.duckdb"
    
    conn = duckdb.connect(str(db_path), read_only=True)
    df = conn.execute(sql).fetchdf()
    conn.close()
    
    return df


def get_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection to the aircraft database.
    
    For advanced users who want direct database access.
    Remember to close the connection when done!
    
    Args:
        read_only: If True, open in read-only mode
    
    Returns:
        DuckDB connection object
    
    Example:
        >>> import hangarbay as hb
        >>> conn = hb.get_connection()
        >>> df = conn.execute("SELECT * FROM aircraft LIMIT 10").fetchdf()
        >>> conn.close()
        
        >>> # Or use context manager:
        >>> with hb.get_connection() as conn:
        ...     df = conn.execute("SELECT * FROM aircraft").fetchdf()
    """
    _ensure_data()
    
    data_dir = config.get_data_dir()
    db_path = data_dir / "publish" / "registry.duckdb"
    
    return duckdb.connect(str(db_path), read_only=read_only)


def status() -> dict:
    """Get information about the current data.
    
    Returns:
        Dictionary with data age, counts, and other metadata
    
    Example:
        >>> import hangarbay as hb
        >>> info = hb.status()
        >>> print(f"Data is {info['age_days']} days old")
        >>> print(f"Total aircraft: {info['aircraft_count']}")
    """
    if not _check_data_exists():
        return {
            "data_exists": False,
            "message": "No data found. Run hb.load_data() to download."
        }
    
    data_dir = config.get_data_dir()
    normalize_meta = data_dir / "publish" / "_meta" / "normalize.json"
    
    with open(normalize_meta) as f:
        meta = json.load(f)
    
    snapshot_date = datetime.fromisoformat(meta["snapshot_date"])
    age_days = (datetime.now() - snapshot_date).days
    
    # Extract row counts from nested structure
    row_counts = meta.get("row_counts", {})
    
    return {
        "data_exists": True,
        "data_dir": str(data_dir),
        "snapshot_date": meta["snapshot_date"],
        "age_days": age_days,
        "is_stale": age_days > 30,
        "aircraft_count": row_counts.get("aircraft", "Unknown"),
        "owners_count": row_counts.get("owners", "Unknown"),
    }


def list_tables() -> list[str]:
    """List all available tables in the database.
    
    Returns:
        List of table names
    
    Example:
        >>> import hangarbay as hb
        >>> tables = hb.list_tables()
        >>> print(tables)
    """
    _ensure_data()
    
    conn = get_connection()
    tables = conn.execute("SHOW TABLES").fetchdf()["name"].tolist()
    conn.close()
    
    return tables


def schema(table_name: str) -> pd.DataFrame:
    """Get the schema for a specific table.
    
    Args:
        table_name: Name of the table
    
    Returns:
        DataFrame with column names and types
    
    Example:
        >>> import hangarbay as hb
        >>> schema_df = hb.schema("aircraft")
        >>> print(schema_df)
    """
    _ensure_data()
    
    conn = get_connection()
    schema_df = conn.execute(f"DESCRIBE {table_name}").fetchdf()
    conn.close()
    
    return schema_df


# Aliases
sync_data = load_data  # Alternative name
update = load_data  # Match CLI naming

