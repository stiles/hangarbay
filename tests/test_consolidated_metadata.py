import json
import pytest
from pathlib import Path
from pipelines.publish import _create_consolidated_metadata


def test_create_consolidated_metadata_success(tmp_path):
    data_root = tmp_path / "data"
    snapshot_date = "2026-01-01"
    publish_dir = tmp_path / "publish"
    # Create directories
    meta_dir = publish_dir / "_meta"
    meta_dir.mkdir(parents=True)
    raw_manifest_dir = data_root / "raw" / snapshot_date
    raw_manifest_dir.mkdir(parents=True)
    # Write normalize metadata
    normalize_meta = {
        "normalized_at": "2026-01-01T00:00:00Z",
        "row_counts": {"aircraft": 1}
    }
    (meta_dir / "normalize.json").write_text(json.dumps(normalize_meta))
    # Write raw manifest
    raw_manifest = {
        "created_at": "2026-01-01T00:00:00Z",
        "files": {
            "file1": {
                "url": "http://example.com/file1",
                "sha256": "abc",
                "size_bytes": 100,
                "filename": "file1"
            }
        },
        "schema_hashes": {}
    }
    (raw_manifest_dir / "manifest.json").write_text(json.dumps(raw_manifest))
    # Create dummy database files
    publish_dir.mkdir(parents=True, exist_ok=True)
    duckdb_path = publish_dir / "dummy.duckdb"
    sqlite_path = publish_dir / "dummy.sqlite"
    duckdb_path.write_bytes(b"\x00" * 1024)
    sqlite_path.write_bytes(b"\x00" * 512)
    # Call function
    result = _create_consolidated_metadata(
        data_root=data_root,
        snapshot_date=snapshot_date,
        publish_dir=publish_dir,
        duckdb_path=duckdb_path,
        sqlite_path=sqlite_path
    )
    assert result["snapshot_date"] == snapshot_date
    assert result["source_urls"] == ["http://example.com/file1"]
    assert "file1" in result["file_hashes"]
    assert result["databases"]["duckdb"]["path"] == duckdb_path.name


def test_create_consolidated_metadata_missing_files(tmp_path):
    data_root = tmp_path / "data"
    snapshot_date = "2026-01-01"
    publish_dir = tmp_path / "publish"
    duckdb_path = publish_dir / "dummy.duckdb"
    sqlite_path = publish_dir / "dummy.sqlite"
    # Ensure no metadata exists
    with pytest.raises(FileNotFoundError):
        _create_consolidated_metadata(
            data_root=data_root,
            snapshot_date=snapshot_date,
            publish_dir=publish_dir,
            duckdb_path=duckdb_path,
            sqlite_path=sqlite_path
        )
