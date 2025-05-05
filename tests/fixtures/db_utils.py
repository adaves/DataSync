"""
Utilities for creating test databases.
"""

import os
import shutil
from pathlib import Path
import pyodbc
from datetime import datetime
import pandas as pd

# Path to the test database template (just create an empty one)
TEST_DB_TEMPLATE = Path(__file__).parent / "test_db_template.txt"

def create_test_db(db_path: Path, table_name: str, data: list[tuple[str, int]]) -> None:
    """
    Create a test Access database with sample data.
    
    Args:
        db_path: Path where to create the database
        table_name: Name of the table to create
        data: List of (date_str, value) tuples
    """
    # Make sure parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create a dummy Access database file
    with open(db_path, 'wb') as f:
        f.write(b'')  # Empty file
    
    # Connect to the database
    conn_str = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={db_path};"
    )
    
    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute(f"""
            CREATE TABLE [{table_name}] (
                [date] DATETIME,
                [value] INT
            )
        """)
        
        # Insert test data
        for date_str, value in data:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            cursor.execute(
                f"INSERT INTO [{table_name}] ([date], [value]) VALUES (?, ?)",
                (date_obj, value)
            )
        
        conn.commit()
        conn.close()
    except pyodbc.Error as e:
        print(f"Error creating test database: {e}")
        raise

def cleanup_test_db(db_path: Path) -> None:
    """Remove test database file."""
    try:
        os.remove(db_path)
    except FileNotFoundError:
        pass 