"""Configuration management for hangarbay."""
import os
from pathlib import Path

# Default data directory
DEFAULT_DATA_DIR = Path.home() / ".hangarbay" / "data"

# Environment variable override
ENV_VAR = "HANGARBAY_DATA_DIR"


def get_data_dir() -> Path:
    """Get the current data directory.
    
    Priority:
    1. Environment variable HANGARBAY_DATA_DIR
    2. Default: ~/.hangarbay/data/
    
    Returns:
        Path: The data directory path
    """
    if ENV_VAR in os.environ:
        return Path(os.environ[ENV_VAR]).expanduser()
    return DEFAULT_DATA_DIR


def set_data_dir(path: str | Path) -> None:
    """Set a custom data directory.
    
    Args:
        path: Path to the data directory
    """
    os.environ[ENV_VAR] = str(Path(path).expanduser().absolute())


def ensure_data_dir() -> Path:
    """Ensure the data directory exists, create if necessary.
    
    Returns:
        Path: The data directory path
    """
    data_dir = get_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir

