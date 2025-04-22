"""
Utilities for creating mock databases for testing.
"""

import os
import pyodbc
from pathlib import Path
from typing import Optional

def create_mock_database(db_path: str, template_path: Optional[str] = None) -> None:
    """
    Create a mock Access database for testing.
    
    Args:
        db_path: Path where the mock database should be created
        template_path: Optional path to a template database to copy from
    """
    db_path = Path(db_path)
    
    # Create parent directory if it doesn't exist
    if not db_path.parent.exists():
        db_path.parent.mkdir(parents=True)
    
    # If template exists, copy it
    if template_path and os.path.exists(template_path):
        import shutil
        shutil.copy2(template_path, db_path)
        return
    
    # Create an empty Access database
    conn_str = (
        "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={db_path};"
        "ExtendedAnsiSQL=1;"
        "UID=Admin;"
        "PWD=;"
    )
    
    # Create empty database file
    with open(db_path, 'wb') as f:
        # Write empty Access database file header
        f.write(b'\x00' * 4096)
    
    # Connect and create basic structure
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        
        # Create test tables
        cursor.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT(50),
                value DOUBLE,
                created_at DATETIME
            )
        """)
        
        cursor.execute("""
            CREATE TABLE yearly_data (
                id INTEGER PRIMARY KEY,
                value DOUBLE,
                time DATETIME
            )
        """)
        
        cursor.execute("""
            CREATE TABLE transaction_test (
                id INTEGER PRIMARY KEY,
                name TEXT(50) NOT NULL,
                value DOUBLE
            )
        """)
        
        conn.commit()
        
    finally:
        conn.close() 