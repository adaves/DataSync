"""
Path handling utilities for consistent path management across the application.
"""

from pathlib import Path
import os
from typing import Union

def normalize_path(path: Union[str, Path]) -> Path:
    """
    Normalize a path to a consistent format.
    
    Args:
        path: Path to normalize (string or Path object)
        
    Returns:
        Normalized Path object
    """
    if isinstance(path, str):
        path = Path(path)
    
    # Convert to absolute path
    path = path.absolute()
    
    # Normalize path separators
    path = Path(os.path.normpath(str(path)))
    
    return path

def format_connection_string_path(path: Union[str, Path]) -> str:
    """
    Format a path for use in a database connection string.
    
    Args:
        path: Path to format (string or Path object)
        
    Returns:
        Formatted path string suitable for connection strings
    """
    path = normalize_path(path)
    
    # Escape backslashes for ODBC connection strings
    return str(path).replace('\\', '\\\\')

def ensure_directory_exists(path: Union[str, Path]) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path (string or Path object)
        
    Returns:
        Path object of the directory
    """
    path = normalize_path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path

def is_valid_database_path(path: Union[str, Path]) -> bool:
    """
    Validate if a path is a valid database file path.
    
    Args:
        path: Path to validate (string or Path object)
        
    Returns:
        True if the path is valid, False otherwise
    """
    path = normalize_path(path)
    return path.exists() and path.suffix.lower() in ['.accdb', '.mdb'] 