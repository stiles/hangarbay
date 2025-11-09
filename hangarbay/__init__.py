"""hangarbay - FAA aircraft registry data pipeline.

Public API for Python notebooks and scripts.

Example usage:
    >>> import hangarbay as hb
    >>> hb.load_data()  # One-time setup
    >>> df = hb.search("N221LA")  # Look up aircraft
    >>> fleet = hb.fleet("United Airlines")  # Find fleet
    >>> df = hb.query("SELECT * FROM aircraft WHERE state='CA'")  # Custom SQL
"""

__version__ = "0.4.0"

# Import public API
from hangarbay.api import (
    fleet,
    get_connection,
    list_tables,
    load_data,
    query,
    schema,
    search,
    status,
    sync_data,
    update,
)
from hangarbay.config import get_data_dir, set_data_dir

__all__ = [
    # Data management
    "load_data",
    "sync_data",
    "update",
    "status",
    # Query functions
    "search",
    "fleet",
    "query",
    # Advanced
    "get_connection",
    "list_tables",
    "schema",
    # Config
    "get_data_dir",
    "set_data_dir",
]

