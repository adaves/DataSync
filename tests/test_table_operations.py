"""
Tests for Access database table operations.
"""
import pytest
from datetime import datetime
from pathlib import Path
import pandas as pd
from app.database.access_utils import AccessDatabaseError
from app.database.table_operations import read_table_data

# Use the actual test database
TEST_DB_PATH = Path('docs/Database11.accdb').absolute()

def test_read_table_data_valid():
    """Test reading data from a valid table with date filter."""
    # We'll use an actual date from your data
    test_date = datetime(2025, 1, 5)
    df = read_table_data(
        db_path=TEST_DB_PATH,
        table_name="YourTableName",  # We'll need the actual table name
        date_column="Time",  # We'll need the actual date column name
        filter_date=test_date
    )
    
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "Time" in df.columns  # Verify date column exists
    assert all(pd.to_datetime(df["Time"]).dt.date == test_date.date())

def test_read_table_invalid_table():
    """Test handling of invalid table name."""
    with pytest.raises(AccessDatabaseError) as exc_info:
        read_table_data(
            db_path=TEST_DB_PATH,
            table_name="NonExistentTable",
            date_column="Time",
            filter_date=datetime(2025, 1, 5)
        )
    assert "table not found" in str(exc_info.value).lower()

def test_read_table_invalid_date_column():
    """Test handling of invalid date column name."""
    with pytest.raises(AccessDatabaseError) as exc_info:
        read_table_data(
            db_path=TEST_DB_PATH,
            table_name="YourTableName",  # We'll need the actual table name
            date_column="NonExistentColumn",
            filter_date=datetime(2025, 1, 5)
        )
    assert "column not found" in str(exc_info.value).lower()

def test_read_table_invalid_date_format():
    """Test handling of invalid date format in the column."""
    with pytest.raises(AccessDatabaseError) as exc_info:
        read_table_data(
            db_path=TEST_DB_PATH,
            table_name="YourTableName",  # We'll need the actual table name
            date_column="NonDateColumn",  # We'll need a column that exists but isn't a date
            filter_date=datetime(2025, 1, 5)
        )
    assert "not a valid date column" in str(exc_info.value).lower()

def test_read_table_no_matching_data():
    """Test handling of valid query that returns no data."""
    df = read_table_data(
        db_path=TEST_DB_PATH,
        table_name="YourTableName",  # We'll need the actual table name
        date_column="Time",
        filter_date=datetime(1900, 1, 1)  # Use a date that shouldn't exist in the data
    )
    
    assert isinstance(df, pd.DataFrame)
    assert df.empty

def test_read_table_large_dataset():
    """Test handling of large dataset (performance test)."""
    import time
    
    start_time = time.time()
    df = read_table_data(
        db_path=TEST_DB_PATH,
        table_name="YourTableName",  # We'll need the actual table name
        date_column="Time",
        filter_date=datetime(2025, 1, 5)
    )
    end_time = time.time()
    
    assert isinstance(df, pd.DataFrame)
    assert (end_time - start_time) < 5  # Should complete in under 5 seconds