"""
Utility functions for working with Microsoft Access databases.
Handles common operations like listing tables, reading data, and managing connections.
"""

import os
from pathlib import Path
from typing import List
import pyodbc
from contextlib import contextmanager


class AccessDatabaseError(Exception):
    """Custom exception for Access database operations."""
    pass


@contextmanager
def access_connection(db_path: Path):
    """
    Context manager for handling Access database connections.
    
    Args:
        db_path (Path): Path to the Access database file
        
    Yields:
        pyodbc.Connection: Active database connection
        
    Raises:
        FileNotFoundError: If database file doesn't exist
        AccessDatabaseError: If connection fails or file is not a valid Access database
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")
    
    if not str(db_path).lower().endswith(('.mdb', '.accdb')):
        raise AccessDatabaseError(f"The file {db_path} is not a valid Access database")
    
    try:
        conn_str = (
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={db_path};"
        )
        conn = pyodbc.connect(conn_str)
        yield conn
    except pyodbc.Error as e:
        error_msg = str(e).lower()
        if any(msg in error_msg for msg in [
            "not a valid database",
            "cannot open database",
            "it may not be a database",
            "file may be corrupt"
        ]):
            raise AccessDatabaseError(f"The file {db_path} is not a valid Access database")
        raise AccessDatabaseError(f"Failed to connect to database: {e}")
    finally:
        try:
            conn.close()
        except (NameError, AttributeError):
            pass


def list_access_tables(db_path: Path | str) -> List[str]:
    """
    List all user tables in an Access database.
    
    Args:
        db_path (Path | str): Path to the Access database file
        
    Returns:
        List[str]: List of table names, excluding system tables
        
    Raises:
        FileNotFoundError: If database file doesn't exist
        AccessDatabaseError: If connection fails or file is not a valid Access database
    """
    db_path = Path(db_path)
    
    with access_connection(db_path) as conn:
        cursor = conn.cursor()
        tables = []
        
        # Get all tables
        for row in cursor.tables():
            # Only include user tables (exclude system tables)
            if row.table_type == 'TABLE' and not row.table_name.startswith('MSys'):
                tables.append(row.table_name)
                
        return sorted(tables)  # Return sorted list for consistency 