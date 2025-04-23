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
    
    # Create a new Access database using DAO
    import win32com.client
    
    # Create the database using ADOX
    cat = win32com.client.Dispatch("ADOX.Catalog")
    conn_str = f"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={db_path};"
    cat.Create(conn_str)
    cat = None  # Release the COM object
    
    # Now connect and create basic structure
    conn_str = (
        "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={db_path};"
        "ExtendedAnsiSQL=1;"
    )
    
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        
        # Create test tables
        cursor.execute("""
            CREATE TABLE test_table (
                id COUNTER PRIMARY KEY,
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