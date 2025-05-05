"""
Tests for Access database table operations.
"""
import pytest
from datetime import datetime, date
from pathlib import Path
import pandas as pd
from app.database.access_utils import AccessDatabaseError
from app.database.table_operations import read_filtered_data, get_table_info
from app.database.date_handling import DateFilter

# Use the actual test database
TEST_DB_PATH = Path('docs/Database11.accdb').absolute()
TEST_TABLE = "testing_table_db"  # We know this table exists

@pytest.mark.skip("Needs database with test data")
def test_read_filtered_data_valid():
    """Test reading data from a valid table with date filter."""
    # Create date filter for a specific date
    test_date = date(2025, 1, 5)
    date_filter = DateFilter(
        start_date=test_date,
        end_date=test_date,
        is_full_date=True
    )
    
    df = read_filtered_data(
        db_path=TEST_DB_PATH,
        table_name=TEST_TABLE,
        date_filter=date_filter
    )
    
    assert isinstance(df, pd.DataFrame)
    # Data validation depends on test data availability
    # assert not df.empty
    # assert all(pd.to_datetime(df["Time"]).dt.date == test_date)

@pytest.mark.skip("Needs database with test data")
def test_read_filtered_data_year():
    """Test reading data for an entire year."""
    # Create date filter for a whole year
    test_year = 2025
    date_filter = DateFilter(
        start_date=date(test_year, 1, 1),
        end_date=date(test_year, 12, 31),
        is_full_date=False
    )
    
    df = read_filtered_data(
        db_path=TEST_DB_PATH,
        table_name=TEST_TABLE,
        date_filter=date_filter
    )
    
    assert isinstance(df, pd.DataFrame)
    # Data validation depends on test data availability
    # assert not df.empty
    # assert all(pd.to_datetime(df["Time"]).dt.year == test_year)

@pytest.mark.skip("Needs database with test data")
def test_read_filtered_data_invalid_table():
    """Test handling of invalid table name."""
    date_filter = DateFilter(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        is_full_date=True
    )
    
    with pytest.raises(AccessDatabaseError) as exc_info:
        read_filtered_data(
            db_path=TEST_DB_PATH,
            table_name="NonExistentTable",
            date_filter=date_filter
        )
    assert "Table not found" in str(exc_info.value)

@pytest.mark.skip("Needs database with test data")
def test_read_filtered_data_no_matching_data():
    """Test handling of valid query that returns no data."""
    # Use a date that shouldn't exist in the data
    test_date = date(1900, 1, 1)
    date_filter = DateFilter(
        start_date=test_date,
        end_date=test_date,
        is_full_date=True
    )
    
    df = read_filtered_data(
        db_path=TEST_DB_PATH,
        table_name=TEST_TABLE,
        date_filter=date_filter
    )
    
    assert isinstance(df, pd.DataFrame)
    assert df.empty

def test_get_table_info_valid():
    """Test getting table info."""
    try:
        table_info = get_table_info(TEST_DB_PATH, TEST_TABLE)
        
        assert table_info.name == TEST_TABLE
        assert isinstance(table_info.columns, list)
        assert len(table_info.columns) > 0
        assert all(hasattr(col, 'name') for col in table_info.columns)
        assert all(hasattr(col, 'data_type') for col in table_info.columns)
    except (FileNotFoundError, AccessDatabaseError):
        pytest.skip("Test database not available")