"""
Tests for Access database table metadata functionality.
"""
import pytest
from pathlib import Path
from datetime import datetime
from app.database.table_operations import get_table_info, TableInfo, ColumnInfo
from app.database.access_utils import AccessDatabaseError

# Use the actual test database
TEST_DB_PATH = Path('docs/Database11.accdb').absolute()
TEST_TABLE = "testing_table_db"  # We know this table exists

def test_get_table_info_valid():
    """Test getting metadata from a valid table."""
    table_info = get_table_info(TEST_DB_PATH, TEST_TABLE)
    
    # Verify TableInfo structure
    assert isinstance(table_info, TableInfo)
    assert table_info.name == TEST_TABLE
    assert len(table_info.columns) > 0
    
    # Verify Time column exists and is datetime
    time_col = next(col for col in table_info.columns if col.name == "Time")
    assert time_col.data_type.lower() in ("datetime", "date/time")
    
    # Verify column info structure
    for col in table_info.columns:
        assert isinstance(col, ColumnInfo)
        assert col.name
        assert col.data_type
        assert isinstance(col.is_nullable, bool)

def test_get_table_info_invalid_table():
    """Test handling of invalid table name."""
    with pytest.raises(AccessDatabaseError) as exc_info:
        get_table_info(TEST_DB_PATH, "NonExistentTable")
    assert "table not found" in str(exc_info.value).lower()

def test_get_table_info_caching():
    """Test that metadata is cached and reused."""
    # First call should hit the database
    first_call = get_table_info(TEST_DB_PATH, TEST_TABLE)
    
    # Second call should use cache
    second_call = get_table_info(TEST_DB_PATH, TEST_TABLE)
    
    # Both calls should return the same object (identity check)
    assert first_call is second_call

def test_get_table_info_cache_different_tables():
    """Test that cache works separately for different tables."""
    # Get info for our test table
    table1_info = get_table_info(TEST_DB_PATH, TEST_TABLE)
    
    # Try with a non-existent table (should not affect cache of first table)
    with pytest.raises(AccessDatabaseError):
        get_table_info(TEST_DB_PATH, "NonExistentTable")
    
    # Get info for test table again - should return cached copy
    table2_info = get_table_info(TEST_DB_PATH, TEST_TABLE)
    
    # Should be the same object
    assert table1_info is table2_info

def test_get_table_info_invalid_database():
    """Test handling of invalid database path."""
    with pytest.raises(FileNotFoundError):
        get_table_info(Path("nonexistent.accdb"), TEST_TABLE) 