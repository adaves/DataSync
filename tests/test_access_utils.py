import pytest
import os
from pathlib import Path
from app.database.access_utils import list_access_tables, AccessDatabaseError

# Use the actual test database
TEST_DB_PATH = Path('docs/Database11.accdb').absolute()

def test_list_access_tables_valid():
    """Test listing tables from the actual Access database."""
    tables = list_access_tables(TEST_DB_PATH)
    assert isinstance(tables, list)
    assert all(isinstance(t, str) for t in tables)
    assert len(tables) > 0  # Should find at least one table

def test_list_access_tables_file_not_found():
    """Test handling of non-existent database file."""
    with pytest.raises(FileNotFoundError):
        list_access_tables('nonexistent_file.accdb')

def test_list_access_tables_invalid_file():
    """Test handling of invalid database file."""
    invalid_path = Path('tests/fixtures/not_a_db.txt')
    invalid_path.parent.mkdir(parents=True, exist_ok=True)
    invalid_path.write_text('This is not an Access DB')
    
    with pytest.raises(AccessDatabaseError) as exc_info:
        list_access_tables(invalid_path)
    error_msg = str(exc_info.value).lower()
    # Check for either the extension check message or the ODBC error message
    assert any(msg in error_msg for msg in [
        'not a valid access database',
        'cannot open database',
        'may not be a database',
        'file may be corrupt'
    ])

def test_list_access_tables_excludes_system_tables():
    """Test that system tables are excluded from the results."""
    tables = list_access_tables(TEST_DB_PATH)
    assert all(not table.startswith('MSys') for table in tables) 