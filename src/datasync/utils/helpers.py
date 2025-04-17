"""
Helper functions module.
"""

from typing import Any, Dict, List
import datetime
from pathlib import Path

def format_timestamp(timestamp: datetime.datetime) -> str:
    """Format a timestamp as a string."""
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")

def safe_get(dictionary: Dict, key: str, default: Any = None) -> Any:
    """Safely get a value from a dictionary."""
    return dictionary.get(key, default)

def ensure_directory(directory: Path) -> None:
    """Ensure a directory exists."""
    directory.mkdir(parents=True, exist_ok=True)

def chunk_list(items: List, chunk_size: int) -> List[List]:
    """Split a list into chunks of specified size."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)] 