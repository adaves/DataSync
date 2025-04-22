"""
Path handling utilities for the DataSync package.
"""

from pathlib import Path
import os
from typing import Union

def normalize_path(path: Union[str, Path]) -> Path:
    """
    Normalize a path to a consistent format.
    
    Args:
        path: The path to normalize (string or Path object)
        
    Returns:
        Path: A normalized Path object with absolute path
    """
    if isinstance(path, str):
        path = Path(path)
    return path.absolute()

def path_to_connection_string(path: Union[str, Path]) -> str:
    """
    Convert a path to a format suitable for database connection strings.
    
    Args:
        path: The path to convert (string or Path object)
        
    Returns:
        str: The path formatted for use in connection strings
    """
    normalized_path = normalize_path(path)
    return str(normalized_path).replace("\\", "\\\\")

def format_connection_string_path(path: Union[str, Path]) -> str:
    """
    Format a path for use in a database connection string.
    
    Args:
        path: Path to format (string or Path object)
        
    Returns:
        Formatted path string suitable for connection strings
    """
    path = normalize_path(path)
    
    # Convert to raw string format with double backslashes
    path_str = str(path)
    # Replace single backslashes with double backslashes
    # We need to escape twice: once for Python string literals, once for ODBC
    return path_str.replace('\\', '\\\\')

def ensure_directory_exists(path: Union[str, Path]) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: The directory path to ensure exists
        
    Returns:
        Path: The normalized path to the directory
    """
    dir_path = normalize_path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path

def is_valid_database_path(path: Union[str, Path]) -> bool:
    """
    Check if a path points to a valid database file.
    
    Args:
        path: The path to check
        
    Returns:
        bool: True if the path exists and has a valid database extension
    """
    path = normalize_path(path)
    return (
        path.exists() and 
        path.is_file() and 
        path.suffix.lower() in ['.mdb', '.accdb']
    )

def get_relative_path(path: Union[str, Path], base_path: Union[str, Path] = None) -> Path:
    """
    Get a path relative to a base path.
    
    Args:
        path: The path to convert to relative
        base_path: The base path to make it relative to (defaults to current working directory)
        
    Returns:
        Path: The relative path
    """
    path = normalize_path(path)
    if base_path is None:
        base_path = Path.cwd()
    else:
        base_path = normalize_path(base_path)
    return path.relative_to(base_path)

def join_paths(*paths: Union[str, Path]) -> Path:
    """
    Join multiple path components together.
    
    Args:
        *paths: Path components to join
        
    Returns:
        Path: The joined path
    """
    return Path(*[str(p) for p in paths])

def get_file_extension(path: Union[str, Path]) -> str:
    """
    Get the file extension from a path.
    
    Args:
        path: The path to get the extension from
        
    Returns:
        str: The file extension (lowercase, with dot)
    """
    return Path(path).suffix.lower() 