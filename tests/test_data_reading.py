"""
Tests for reading and filtering data from Access databases.
"""

import pytest
from pathlib import Path
import pandas as pd
from datetime import date
from src.app.database.date_handling import DateFilter
from src.app.database.table_operations import read_filtered_data
from src.app.database.access_utils import AccessDatabaseError
from tests.fixtures.db_utils import create_test_db, cleanup_test_db

# Test data
TEST_DATA = [
    # (date, value)
    ('2024-01-01', 1),
    ('2024-01-02', 2),
    ('2024-01-03', 3),
    ('2024-02-01', 4),
    ('2024-02-02', 5),
    ('2024-03-01', 6),
    ('2025-01-01', 7),
    ('2025-01-02', 8),
]

@pytest.fixture
def test_db_path(tmp_path):
    """Create a temporary test database with sample data."""
    db_path = tmp_path / "test_db.accdb"
    create_test_db(db_path, "test_table", TEST_DATA)
    yield db_path
    cleanup_test_db(db_path)

@pytest.fixture
def full_date_filter():
    """Create a DateFilter for a specific date."""
    return DateFilter(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        is_full_date=True
    )

@pytest.fixture
def year_filter():
    """Create a DateFilter for a specific year."""
    return DateFilter(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        is_full_date=False
    )

def test_read_filtered_data_full_date(test_db_path, full_date_filter):
    """Test reading data with a full date filter."""
    df = read_filtered_data(test_db_path, "test_table", full_date_filter)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]['value'] == 2
    assert df.iloc[0]['date'].date() == date(2024, 1, 2)

def test_read_filtered_data_year(test_db_path, year_filter):
    """Test reading data with a year filter."""
    df = read_filtered_data(test_db_path, "test_table", year_filter)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 6  # All 2024 records
    assert all(df['date'].dt.year == 2024)

def test_read_filtered_data_empty_result(test_db_path):
    """Test reading data that returns no results."""
    filter_2026 = DateFilter(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        is_full_date=False
    )
    
    df = read_filtered_data(test_db_path, "test_table", filter_2026)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

def test_read_filtered_data_invalid_table(test_db_path, full_date_filter):
    """Test reading from non-existent table."""
    with pytest.raises(AccessDatabaseError, match="Table not found"):
        read_filtered_data(test_db_path, "non_existent_table", full_date_filter)

def test_read_filtered_data_chunking(test_db_path, year_filter):
    """Test reading data in chunks."""
    progress_updates = []
    
    def progress_callback(current: int, total: int):
        progress_updates.append((current, total))
    
    df = read_filtered_data(
        test_db_path,
        "test_table",
        year_filter,
        chunk_size=2,
        progress_callback=progress_callback
    )
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 6
    assert len(progress_updates) > 0
    assert progress_updates[-1][0] == progress_updates[-1][1]  # Final update should be complete

def test_read_filtered_data_memory_usage(test_db_path, year_filter):
    """Test memory usage with large dataset."""
    import psutil
    import os
    
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss
    
    df = read_filtered_data(test_db_path, "test_table", year_filter)
    
    final_memory = process.memory_info().rss
    memory_increase = final_memory - initial_memory
    
    # Memory increase should be reasonable (less than 100MB)
    assert memory_increase < 100 * 1024 * 1024 