"""
Path handling utilities for the DataSync package.
"""

from pathlib import Path
import os
from typing import Union, Optional

def normalize_path(path: Union[str, Path]) -> Path:
    """
    Normalize a path to a consistent format.
    
    Args:
        path: The path to normalize (string or Path object)
        
    Returns:
        Path: A normalized Path object with absolute path
        
    Raises:
        TypeError: If path is None
        ValueError: If path is empty
    """
    if path is None:
        raise TypeError("Path cannot be None")
    if isinstance(path, str) and not path:
        raise ValueError("Path cannot be empty")
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
        
    Raises:
        TypeError: If path is None
        ValueError: If path is not an Access database file
    """
    normalized_path = normalize_path(path)
    if not is_valid_database_path(normalized_path):
        raise ValueError("Path must point to a valid Access database file (.mdb or .accdb)")
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
        
    Raises:
        TypeError: If path is None
        ValueError: If path is empty
        FileExistsError: If path exists and is a file
        OSError: If directory cannot be created
    """
    dir_path = normalize_path(path)
    if dir_path.exists():
        if dir_path.is_file():
            raise FileExistsError(f"Path exists and is a file: {dir_path}")
        return dir_path
    
    # Handle long paths on Windows
    if os.name == 'nt':
        # Convert to extended path format
        extended_path = f"\\\\?\\{dir_path}"
        try:
            # Try to create the directory with extended path prefix
            os.makedirs(extended_path, exist_ok=True)
            return dir_path
        except OSError:
            # If extended path fails, try creating parent directories first
            try:
                parent = dir_path.parent
                while not parent.exists():
                    try:
                        os.makedirs(f"\\\\?\\{parent}", exist_ok=True)
                    except OSError:
                        # If extended path fails for parent, try regular mkdir
                        parent.mkdir(parents=True, exist_ok=True)
                    parent = parent.parent
                try:
                    os.makedirs(f"\\\\?\\{dir_path}", exist_ok=True)
                except OSError:
                    # If extended path fails for target, try regular mkdir
                    dir_path.mkdir(exist_ok=True)
                return dir_path
            except OSError as e:
                raise OSError(f"Failed to create directory {dir_path}: {e}")
    else:
        try:
            dir_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise OSError(f"Failed to create directory {dir_path}: {e}")
    return dir_path

def is_valid_database_path(path: Union[str, Path]) -> bool:
    """
    Check if a path points to a valid database file.
    
    Args:
        path: The path to check
        
    Returns:
        bool: True if the path exists and has a valid database extension
        
    Raises:
        TypeError: If path is None
        ValueError: If path is empty
    """
    if path is None:
        raise TypeError("Path cannot be None")
    if isinstance(path, str) and not path:
        raise ValueError("Path cannot be empty")
    try:
        path = normalize_path(path)
        return (
            path.exists() and 
            path.is_file() and 
            path.suffix.lower() in ['.mdb', '.accdb']
        )
    except Exception:
        return False

def get_relative_path(path: Union[str, Path], base_path: Optional[Union[str, Path]] = None) -> Path:
    """
    Get a path relative to a base path.
    
    Args:
        path: The path to convert to relative
        base_path: The base path to make it relative to (defaults to current working directory)
        
    Returns:
        Path: The relative path
        
    Raises:
        TypeError: If path is None
        ValueError: If path is empty or not relative to base_path
    """
    path = normalize_path(path)
    if base_path is None:
        base_path = Path.cwd()
    else:
        base_path = normalize_path(base_path)
    try:
        # Resolve both paths to handle . and .. components
        resolved_path = path.resolve()
        resolved_base = base_path.resolve()
        # Get the relative path
        rel_path = resolved_path.relative_to(resolved_base)
        # Preserve case on Windows
        if os.name == 'nt':
            # Get the parts after the base path while preserving case
            rel_parts = []
            # Use the original path parts to preserve case
            path_parts = list(path.parts)
            base_parts = list(base_path.parts)
            # Find where the base path ends in the original path
            i = 0
            while i < len(base_parts) and i < len(path_parts):
                if base_parts[i].lower() != path_parts[i].lower():
                    raise ValueError(f"Path {path} is not relative to {base_path}")
                i += 1
            # Get the remaining parts from the original path
            rel_parts = path_parts[i:]
            # Create a new path from the remaining parts
            rel_path = Path(*rel_parts)
        return rel_path
    except ValueError:
        raise ValueError(f"Path {path} is not relative to {base_path}")

def join_paths(*paths: Union[str, Path]) -> Path:
    """
    Join multiple path components together.
    
    Args:
        *paths: Path components to join
        
    Returns:
        Path: The joined path
        
    Raises:
        TypeError: If any path component is None
    """
    if any(p is None for p in paths):
        raise TypeError("Path components cannot be None")
    return Path(*[str(p) for p in paths])

def get_file_extension(path: Union[str, Path]) -> str:
    """
    Get the file extension from a path.
    
    Args:
        path: The path to get the extension from
        
    Returns:
        str: The file extension (lowercase, with dot)
        
    Raises:
        TypeError: If path is None
    """
    if path is None:
        raise TypeError("Path cannot be None")
    if isinstance(path, str) and not path:
        return ""
    return Path(path).suffix.lower() 