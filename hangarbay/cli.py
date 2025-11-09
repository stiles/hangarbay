"""CLI interface for hangarbay (command: hangar)."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional
import pandas as pd
import typer
from rich.console import Console

app = typer.Typer(
    name="hangar",
    help="FAA aircraft registry workflow tool",
    add_completion=False,
)
console = Console()


def get_data_age_info(data_root: Path = Path("data")) -> Optional[dict]:
    """
    Get information about the current data age.
    
    Returns:
        dict with 'snapshot_date', 'days_old', 'age_warning' or None if no data
    """
    manifest_path = data_root / "publish" / "_meta" / "normalize.json"
    
    if not manifest_path.exists():
        return None
    
    try:
        with open(manifest_path) as f:
            meta = json.load(f)
        
        snapshot_date_str = meta.get("snapshot_date", "unknown")
        if snapshot_date_str == "unknown":
            return None
        
        snapshot_date = datetime.fromisoformat(snapshot_date_str)
        days_old = (datetime.now() - snapshot_date).days
        
        return {
            "snapshot_date": snapshot_date_str,
            "days_old": days_old,
            "age_warning": days_old >= 30
        }
    except Exception:
        return None


def show_age_warning(skip_check: bool = False):
    """Show warning if data is stale (30+ days old)."""
    if skip_check:
        return
    
    age_info = get_data_age_info()
    if age_info and age_info["age_warning"]:
        console.print(
            f"[yellow]⚠️  Data is {age_info['days_old']} days old "
            f"(last updated: {age_info['snapshot_date']})[/yellow]"
        )
        console.print(f"[yellow]   Run 'hangar update' to fetch the latest FAA data[/yellow]\n")


@app.command()
def fetch(
    data_root: Path = typer.Option(Path("data"), help="Root data directory"),
    snapshot_date: Optional[str] = typer.Option(None, help="Snapshot date (YYYY-MM-DD)"),
):
    """Download latest FAA registry files and create manifest."""
    from pipelines.fetch import fetch as fetch_pipeline
    
    try:
        raw_dir = fetch_pipeline(data_root=data_root, snapshot_date=snapshot_date)
        console.print(f"[green]Fetch complete: {raw_dir}[/green]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(code=1)


@app.command()
def normalize(
    data_root: Path = typer.Option(Path("data"), help="Root data directory"),
    snapshot_date: Optional[str] = typer.Option(None, help="Snapshot date (YYYY-MM-DD)"),
):
    """Normalize raw files to typed Parquet tables."""
    from pipelines.normalize import normalize as normalize_pipeline
    
    try:
        publish_dir = normalize_pipeline(data_root=data_root, snapshot_date=snapshot_date)
        console.print(f"[green]Normalize complete: {publish_dir}[/green]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(code=1)


@app.command()
def publish(
    data_root: Path = typer.Option(Path("data"), help="Root data directory"),
):
    """Publish Parquet tables to DuckDB and SQLite FTS."""
    from pipelines.publish import publish as publish_pipeline
    
    try:
        publish_dir = publish_pipeline(data_root=data_root)
        console.print(f"[green]Publish complete: {publish_dir}[/green]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(code=1)


@app.command()
def update(
    data_root: Path = typer.Option(Path("data"), help="Root data directory"),
):
    """Update all data: fetch → normalize → publish (full pipeline)."""
    from pipelines.fetch import fetch as fetch_pipeline
    from pipelines.normalize import normalize as normalize_pipeline
    from pipelines.publish import publish as publish_pipeline
    
    console.print("[bold cyan]Running full update pipeline...[/bold cyan]\n")
    
    try:
        # Step 1: Fetch
        console.print("[cyan]Step 1/3: Fetching FAA data...[/cyan]")
        raw_dir = fetch_pipeline(data_root=data_root)
        console.print(f"[green]✓ Fetch complete[/green]\n")
        
        # Step 2: Normalize
        console.print("[cyan]Step 2/3: Normalizing data...[/cyan]")
        publish_dir = normalize_pipeline(data_root=data_root)
        console.print(f"[green]✓ Normalize complete[/green]\n")
        
        # Step 3: Publish
        console.print("[cyan]Step 3/3: Publishing to databases...[/cyan]")
        publish_dir = publish_pipeline(data_root=data_root)
        console.print(f"[green]✓ Publish complete[/green]\n")
        
        console.print("[bold green]✓ Update complete! Data is now current.[/bold green]")
        
    except Exception as e:
        console.print(f"[red]Error during update: {e}[/red]")
        raise typer.Exit(code=1)


@app.command()
def status(
    data_root: Path = typer.Option(Path("data"), help="Root data directory"),
):
    """Show current data status and age."""
    from rich.table import Table
    
    age_info = get_data_age_info(data_root)
    
    if not age_info:
        console.print("[yellow]No data found. Run 'hangar update' to fetch FAA data.[/yellow]")
        return
    
    # Build status table
    table = Table(show_header=False, box=None)
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="white")
    
    table.add_row("Snapshot Date", age_info["snapshot_date"])
    table.add_row("Days Old", str(age_info["days_old"]))
    
    if age_info["age_warning"]:
        table.add_row("Status", "[yellow]⚠️  Stale (30+ days)[/yellow]")
    else:
        table.add_row("Status", "[green]✓ Current[/green]")
    
    console.print("\n[bold]Data Status[/bold]\n")
    console.print(table)
    
    if age_info["age_warning"]:
        console.print(f"\n[yellow]Run 'hangar update' to fetch the latest FAA data[/yellow]")
    
    console.print()


@app.command()
def sql(
    query: str,
    database: Path = typer.Option(Path("data/publish/registry.duckdb"), help="DuckDB database path"),
    output_format: str = typer.Option("table", help="Output format: table, json, csv"),
    case_insensitive: bool = typer.Option(False, "--case-insensitive", "-i", help="Convert LIKE to ILIKE for case-insensitive matching"),
    skip_age_check: bool = typer.Option(False, "--skip-age-check", help="Skip data age warning"),
):
    """Execute SQL query against the registry database.
    
    By default, LIKE is case-sensitive. Use --case-insensitive (-i) to make it case-insensitive,
    or use ILIKE directly in your query for case-insensitive matching.
    
    Example: hangar sql "SELECT * FROM owners WHERE owner_name_std LIKE '%Boeing%'" -i
    """
    import duckdb
    import re
    
    # Show age warning if data is stale
    show_age_warning(skip_age_check)
    
    if not database.exists():
        console.print(f"[red]Database not found: {database}[/red]")
        console.print("[yellow]Run 'hangar publish' first[/yellow]")
        raise typer.Exit(code=1)
    
    try:
        # Convert LIKE to ILIKE for case-insensitive matching if requested
        if case_insensitive:
            # Replace LIKE with ILIKE (case-insensitive), preserving NOT LIKE -> NOT ILIKE
            query = re.sub(r'\bLIKE\b', 'ILIKE', query, flags=re.IGNORECASE)
            console.print(f"[dim]Using case-insensitive matching (LIKE → ILIKE)[/dim]\n")
        
        conn = duckdb.connect(str(database), read_only=True)
        result = conn.execute(query).fetchdf()
        
        if output_format == "json":
            console.print(result.to_json(orient="records", indent=2))
        elif output_format == "csv":
            console.print(result.to_csv(index=False))
        else:
            # Pretty table output with Rich
            from rich.table import Table
            
            def format_cell_value(val):
                """Format cell values for clean display."""
                val_str = str(val)
                # Replace pandas NA/NaT with empty string
                if val_str in ('<NA>', 'NaT', 'nan', 'None'):
                    return ''
                # Strip time from datetime stamps (keep just date)
                if ' 00:00:00' in val_str:
                    return val_str.replace(' 00:00:00', '')
                return val_str
            
            table = Table(show_header=True, header_style="bold cyan")
            
            # Add columns
            for col in result.columns:
                table.add_column(str(col))
            
            # Add rows (limit to 100 for display)
            for idx, row in result.head(100).iterrows():
                table.add_row(*[format_cell_value(val) for val in row])
            
            if len(result) > 100:
                console.print(f"\n[dim]Showing first 100 of {len(result)} rows[/dim]")
            
            console.print(table)
            console.print(f"\n[dim]{len(result)} rows returned[/dim]")
        
        conn.close()
    except Exception as e:
        console.print(f"[red]Query error: {e}[/red]")
        raise typer.Exit(code=1)


@app.command()
def search(
    n_number: str,
    skip_age_check: bool = typer.Option(False, "--skip-age-check", help="Skip data age warning"),
):
    """Search for a specific N-number registration."""
    import duckdb
    
    # Show age warning if data is stale
    show_age_warning(skip_age_check)
    
    database = Path("data/publish/registry.duckdb")
    if not database.exists():
        console.print(f"[red]Database not found. Run 'hangar publish' first.[/red]")
        raise typer.Exit(code=1)
    
    try:
        # Strip leading "N" if present - FAA stores without it
        search_term = n_number.upper()
        if search_term.startswith('N'):
            search_term = search_term[1:]
        
        conn = duckdb.connect(str(database), read_only=True)
        
        # Get aircraft info with make/model
        query = """
        SELECT 
            a.n_number,
            a.serial_no,
            m.maker,
            m.model,
            a.year_mfr,
            a.reg_status,
            a.status_date,
            a.reg_expiration,
            r.reg_type
        FROM aircraft a
        LEFT JOIN aircraft_make_model m USING(mfr_mdl_code)
        LEFT JOIN registrations r USING(n_number)
        WHERE UPPER(a.n_number) = ?
        """
        
        result = conn.execute(query, [search_term]).fetchdf()
        
        if len(result) == 0:
            console.print(f"[yellow]No aircraft found with N-number: {n_number}[/yellow]")
            console.print(f"[dim](Searched for: {search_term})[/dim]")
            conn.close()
            return
        
        # Get owner info
        owner_query = """
        SELECT owner_name_std, city_std, state_std
        FROM owners
        WHERE UPPER(n_number) = ?
        LIMIT 1
        """
        owner_result = conn.execute(owner_query, [search_term]).fetchdf()
        
        # Format the output nicely
        from rich.panel import Panel
        from rich.table import Table
        from datetime import datetime
        
        row = result.iloc[0]
        display_n_number = f"N{search_term}"
        
        # Decode status codes (from FAA data dictionary)
        status_codes = {
            'V': 'Valid',
            'M': 'Valid - Manufacturer/Dealer',
            'T': 'Valid - Trainee',
            'R': 'Registration Pending',
            'N': 'Non-Citizen Corp (flight hours not reported)',
            'E': 'Revoked by Enforcement',
            'W': 'Invalid/Ineffective',
            'D': 'Expired Dealer',
            'A': 'Triennial Form Mailed',
            'S': 'Second Triennial Form Mailed',
            'X': 'Enforcement Letter',
            'Z': 'Permanent Reserved',
            '1': 'Triennial Form Undeliverable',
            '2': 'N-Number Assigned - Not Yet Registered',
            '3': 'N-Number Assigned (Non Type Certificated) - Not Yet Registered',
            '4': 'N-Number Assigned (Import) - Not Yet Registered',
            '5': 'Reserved N-Number',
            '6': 'Administratively Canceled',
            '7': 'Sale Reported',
            '8': 'Second Triennial Mailed - No Response',
            '9': 'Registration Revoked',
            '10': 'N-Number Assigned - Pending Cancellation',
            '11': 'N-Number Assigned (Amateur) - Pending Cancellation',
            '12': 'N-Number Assigned (Import) - Pending Cancellation',
            '13': 'Registration Expired',
            '14': 'First Notice for Re-Registration',
            '15': 'Second Notice for Re-Registration',
            '16': 'Registration Expired - Pending Cancellation',
            '17': 'Sale Reported - Pending Cancellation',
            '18': 'Sale Reported - Canceled',
            '19': 'Registration Pending - Pending Cancellation',
            '20': 'Registration Pending - Canceled',
            '21': 'Revoked - Pending Cancellation',
            '22': 'Revoked - Canceled',
            '23': 'Expired Dealer - Pending Cancellation',
            '24': 'Third Notice for Re-Registration',
            '25': 'First Notice for Registration Renewal',
            '26': 'Second Notice for Registration Renewal',
            '27': 'Registration Expired',
            '28': 'Third Notice for Registration Renewal',
            '29': 'Registration Expired - Pending Cancellation',
        }
        
        # Type Registrant codes (from FAA data dictionary)
        # Note: These are actually Airworthiness Classification + Operation Codes
        # Format: [Classification][Operation Code(s)]
        # Classification: 1=Standard, 2=Limited, 3=Restricted, 4=Experimental, 
        #                 5=Provisional, 6=Multiple, 7=Primary, 8=Special Flight, 9=Light Sport
        # Common operation codes: N=Normal, U=Utility, A=Acrobatic, T=Transport
        reg_type_codes = {
            # Single digit = Type Registrant (owner type)
            '1': 'Individual',
            '2': 'Partnership',
            '3': 'Corporation',
            '4': 'Co-Owned',
            '5': 'Government',
            '7': 'LLC',
            '8': 'Non-Citizen Corporation',
            '9': 'Non-Citizen Co-Owned',
            # Standard Airworthiness + Operations
            '1N': 'Standard Airworthiness - Normal',
            '1U': 'Standard Airworthiness - Utility',
            '1A': 'Standard Airworthiness - Acrobatic',
            '1T': 'Standard Airworthiness - Transport',
            '1NU': 'Standard Airworthiness - Normal/Utility',
            '1NA': 'Standard Airworthiness - Normal/Acrobatic',
            '1B': 'Standard Airworthiness - Balloon',
            '1G': 'Standard Airworthiness - Glider',
            '1C': 'Standard Airworthiness - Commuter',
            # Experimental (most common)
            '42': 'Experimental - Amateur Built',
            '43': 'Experimental - Exhibition',
            '41': 'Experimental - Research & Development',
            '44': 'Experimental - Racing',
            '45': 'Experimental - Crew Training',
            '47': 'Experimental - Operating Kit Built',
            '48A': 'Experimental - Registered Prior to 01/31/08',
            '48B': 'Experimental - Light-Sport Kit-Built',
            '48C': 'Experimental - Light-Sport Previously Issued Cert',
            '49A': 'Experimental - Unmanned Aircraft R&D',
            '49B': 'Experimental - Unmanned Aircraft Market Survey',
            '49C': 'Experimental - Unmanned Aircraft Crew Training',
            '49D': 'Experimental - Unmanned Aircraft Exhibition',
            # Restricted
            '31': 'Restricted - Agriculture/Pest Control',
            '32': 'Restricted - Aerial Surveying',
            '33': 'Restricted - Aerial Advertising',
            '314': 'Restricted - Forest/Agriculture',
            # Light Sport
            '9A': 'Light Sport - Airplane',
            '9G': 'Light Sport - Glider',
            '9L': 'Light Sport - Lighter than Air',
            # Special permits
            '6131': 'Multiple/Special',
        }
        
        # Format dates nicely
        def format_date(date_val):
            if pd.isna(date_val):
                return "N/A"
            if isinstance(date_val, str):
                date_val = pd.to_datetime(date_val)
            return date_val.strftime("%b %d, %Y")
        
        # Build the display
        console.print(f"\n[bold cyan]Aircraft Registration: {display_n_number}[/bold cyan]\n")
        
        # Owner section (first!)
        if len(owner_result) > 0:
            owner = owner_result.iloc[0]
            if pd.notna(owner['owner_name_std']):
                console.print(f"[bold]Owner:[/bold] {owner['owner_name_std']}")
            if pd.notna(owner['city_std']) and pd.notna(owner['state_std']):
                console.print(f"[bold]Location:[/bold] {owner['city_std']}, {owner['state_std']}")
            console.print()
        
        # Aircraft details table
        details = Table(show_header=False, box=None, padding=(0, 2))
        details.add_column("Field", style="cyan", no_wrap=True)
        details.add_column("Value", style="white")
        
        # Make/Model section
        if pd.notna(row['maker']) or pd.notna(row['model']):
            make_model = []
            if pd.notna(row['maker']) and row['maker']:
                make_model.append(str(row['maker']))
            if pd.notna(row['model']) and row['model']:
                make_model.append(str(row['model']))
            if make_model:
                details.add_row("Make & Model:", " ".join(make_model))
        
        if pd.notna(row['year_mfr']):
            details.add_row("Year Manufactured:", str(int(row['year_mfr'])))
        
        if pd.notna(row['serial_no']) and row['serial_no']:
            details.add_row("Serial Number:", str(row['serial_no']))
        
        # Registration details
        if pd.notna(row['reg_status']) and row['reg_status']:
            status_code = str(row['reg_status']).strip()
            status_text = status_codes.get(status_code, status_code)
            details.add_row("Registration Status:", status_text)
        
        if pd.notna(row['reg_type']) and row['reg_type']:
            reg_code = str(row['reg_type']).strip()
            reg_text = reg_type_codes.get(reg_code, f"Code: {reg_code}")
            details.add_row("Certificate Type:", reg_text)
        
        if pd.notna(row['status_date']):
            details.add_row("Status Date:", format_date(row['status_date']))
        
        if pd.notna(row['reg_expiration']):
            details.add_row("Expiration:", format_date(row['reg_expiration']))
        
        console.print(details)
        console.print()
        conn.close()
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(code=1)


@app.command()
def version():
    """Show hangarbay version."""
    from hangarbay import __version__
    console.print(f"hangarbay version {__version__}")


if __name__ == "__main__":
    app()

