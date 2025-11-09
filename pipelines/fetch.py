"""Fetch FAA registry files and create manifest with provenance."""

import hashlib
import json
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional
import requests
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()

# Global quiet flag
_quiet = False

# FAA ReleasableAircraft download URL (single zip file containing all data)
FAA_ZIP_URL = "https://registry.faa.gov/database/ReleasableAircraft.zip"

# Files we want to extract from the zip
FAA_FILES_TO_EXTRACT = ["MASTER.txt", "ACFTREF.txt", "ENGINE.txt"]


def compute_sha256(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def download_file(url: str, dest_path: Path, retries: int = 3) -> bool:
    """Download a file with retries and progress indication."""
    # Use browser-like headers to avoid being blocked by government servers
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    
    for attempt in range(retries):
        try:
            if not _quiet: console.print(f"[cyan]Downloading {dest_path.name}...[/cyan]")
            response = requests.get(url, stream=True, timeout=180, headers=headers)
            response.raise_for_status()
            
            total_size = int(response.headers.get("content-length", 0))
            
            with open(dest_path, "wb") as f:
                if total_size == 0:
                    f.write(response.content)
                else:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
            
            if not _quiet: console.print(f"[green]✓ Downloaded {dest_path.name} ({total_size:,} bytes)[/green]")
            return True
            
        except Exception as e:
            if not _quiet: console.print(f"[yellow]Attempt {attempt + 1}/{retries} failed: {e}[/yellow]")
            if attempt == retries - 1:
                if not _quiet: console.print(f"[red]✗ Failed to download {dest_path.name}[/red]")
                return False
    
    return False


def create_manifest(
    raw_dir: Path,
    files_info: dict[str, dict],
    snapshot_date: str,
    previous_snapshot: Optional[str] = None,
) -> None:
    """Create manifest.json with provenance metadata."""
    from hangarbay.schemas import get_all_schema_hashes
    
    manifest = {
        "snapshot_date": snapshot_date,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "previous_snapshot": previous_snapshot,
        "files": files_info,
        "schema_hashes": get_all_schema_hashes(),
    }
    
    manifest_path = raw_dir / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    
    if not _quiet: console.print(f"[green]✓ Created manifest at {manifest_path}[/green]")


def fetch(
    data_root: Path = Path("data"),
    snapshot_date: Optional[str] = None,
    quiet: bool = False,
) -> Path:
    """
    Fetch FAA registry files and create a dated snapshot with manifest.
    
    Args:
        data_root: Root data directory (default: data/)
        snapshot_date: Optional explicit date (default: today YYYY-MM-DD)
        quiet: Suppress console output
    
    Returns:
        Path to the created raw snapshot directory
    """
    global _quiet
    _quiet = quiet
    
    if snapshot_date is None:
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
    
    raw_dir = data_root / "raw" / snapshot_date
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    if not _quiet:
        if not _quiet: console.print(f"\n[bold cyan]Fetching FAA registry data for {snapshot_date}[/bold cyan]\n")
    
    # Download the ZIP file
    zip_path = raw_dir / "ReleasableAircraft.zip"
    
    if not zip_path.exists():
        success = download_file(FAA_ZIP_URL, zip_path)
        if not success:
            if not _quiet: console.print(f"[red]Failed to download FAA zip file, aborting[/red]")
            return raw_dir
    else:
        if not _quiet: console.print(f"[yellow]ZIP file already exists, skipping download[/yellow]")
    
    # Extract desired files from the ZIP
    if not _quiet: console.print(f"[cyan]Extracting files from ZIP...[/cyan]")
    files_info = {}
    
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for filename in FAA_FILES_TO_EXTRACT:
                dest_path = raw_dir / filename
                
                if not dest_path.exists():
                    if not _quiet: console.print(f"[cyan]Extracting {filename}...[/cyan]")
                    zf.extract(filename, raw_dir)
                    if not _quiet: console.print(f"[green]✓ Extracted {filename}[/green]")
                else:
                    if not _quiet: console.print(f"[yellow]{filename} already extracted[/yellow]")
                
                # Compute hash and record metadata
                sha256 = compute_sha256(dest_path)
                file_size = dest_path.stat().st_size
                
                # Remove .txt extension for the key name
                name = filename.replace(".txt", "")
                files_info[name] = {
                    "filename": filename,
                    "url": FAA_ZIP_URL,
                    "sha256": sha256,
                    "size_bytes": file_size,
                }
                
                if not _quiet: console.print(f"[dim]  SHA256: {sha256}[/dim]")
    
    except zipfile.BadZipFile:
        if not _quiet: console.print(f"[red]Error: Downloaded file is not a valid ZIP file[/red]")
        return raw_dir
    except KeyError as e:
        if not _quiet: console.print(f"[red]Error: File {e} not found in ZIP archive[/red]")
        return raw_dir
    
    # Check for previous snapshot
    previous_snapshot = None
    raw_root = data_root / "raw"
    if raw_root.exists():
        existing = sorted([d.name for d in raw_root.iterdir() if d.is_dir()])
        if len(existing) > 1:
            previous_snapshot = existing[-2]  # Second-to-last is previous
    
    # Create manifest
    create_manifest(raw_dir, files_info, snapshot_date, previous_snapshot)
    
    if not _quiet: console.print(f"\n[bold green]✓ Fetch complete![/bold green]")
    if not _quiet: console.print(f"[dim]Snapshot saved to: {raw_dir}[/dim]\n")
    
    return raw_dir


if __name__ == "__main__":
    # Can be run directly: python -m pipelines.fetch
    fetch()

