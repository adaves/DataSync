"""
Tests for Access database delete operations.
"""
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from datetime import datetime, date
from app.database.access_utils import AccessDatabaseError
from app.database.delete_operations import delete_data_by_date, get_temp_table_name, cleanup_old_temp_tables
from app.database.date_handling import DateFilter

def test_get_temp_table_name():
    """Test generation of temporary table names."""
    base_table = "test_table"
    test_date = date(2025, 1, 5)
    
    temp_name = get_temp_table_name(base_table, test_date)
    assert isinstance(temp_name, str)
    assert temp_name == f"{base_table}_1_5_2025_temp_table"
    
    # Test with different date formats
    test_date2 = date(2025, 12, 31)
    temp_name2 = get_temp_table_name(base_table, test_date2)
    assert temp_name2 == f"{base_table}_12_31_2025_temp_table"

@patch('app.database.delete_operations.get_table_info')
@patch('app.database.delete_operations.access_connection')
def test_delete_data_by_date_full_date(mock_connection, mock_get_table_info):
    """Test deleting data for a specific date."""
    # Setup mocks
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_connection.return_value = mock_conn
    
    # Mock fetch operations
    mock_cursor.fetchone.side_effect = [(5,), (5,)]  # First for count, second for verification
    
    # Mock table info
    column1 = MagicMock()
    column1.name = "date"
    column1.data_type = "DATETIME"
    column2 = MagicMock()
    column2.name = "value"
    column2.data_type = "INTEGER"
    mock_table_info = MagicMock()
    mock_table_info.columns = [column1, column2]
    mock_get_table_info.return_value = mock_table_info
    
    # Setup test data
    test_date = date(2023, 1, 5)
    date_filter = DateFilter(
        start_date=test_date,
        end_date=test_date,
        is_full_date=True
    )
    
    # Call function
    temp_table = delete_data_by_date(
        db_path=Path("dummy.accdb"),
        table_name="test_table",
        date_filter=date_filter,
        date_column="date"
    )
    
    # Assert results
    assert temp_table == f"test_table_1_5_2023_temp_table"
    
    # Verify correct SQL was executed
    assert mock_cursor.execute.call_count == 5
    
    # Verify SQL operations without checking exact SQL strings
    calls = [str(call) for call in mock_cursor.execute.call_args_list]
    assert any("SELECT COUNT(*)" in call and "test_table" in call for call in calls)
    assert any("CREATE TABLE" in call for call in calls)
    assert any("INSERT INTO" in call and "test_table_1_5_2023_temp_table" in call for call in calls)
    assert any("DELETE FROM" in call and "test_table" in call for call in calls)
    assert any("SELECT COUNT(*)" in call and "test_table_1_5_2023_temp_table" in call for call in calls)

@patch('app.database.delete_operations.get_table_info')
@patch('app.database.delete_operations.access_connection')
def test_delete_data_by_date_no_data(mock_connection, mock_get_table_info):
    """Test deleting when no data exists for the date."""
    # Setup mocks
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_connection.return_value = mock_conn
    
    # Mock fetch operations - return 0 for count
    mock_cursor.fetchone.return_value = (0,)
    
    # Mock table info
    column1 = MagicMock()
    column1.name = "date"
    column1.data_type = "DATETIME"
    mock_table_info = MagicMock()
    mock_table_info.columns = [column1]
    mock_get_table_info.return_value = mock_table_info
    
    # Setup test data for a future date with no data
    test_date = date(2099, 1, 1)
    date_filter = DateFilter(
        start_date=test_date,
        end_date=test_date,
        is_full_date=True
    )
    
    # Test should raise exception
    with pytest.raises(AccessDatabaseError) as exc_info:
        delete_data_by_date(
            db_path=Path("dummy.accdb"),
            table_name="test_table",
            date_filter=date_filter,
            date_column="date"
        )
    
    assert "No data found" in str(exc_info.value)
    
    # Verify only the count SQL was executed
    assert mock_cursor.execute.call_count == 1
    
    # Check the SQL includes the correct date filter
    where_clause = date_filter.get_where_clause("date")
    calls = mock_cursor.execute.call_args_list
    assert f"SELECT COUNT(*) FROM [test_table] WHERE {where_clause}" in str(calls[0])

@patch('app.database.delete_operations.get_table_info')
def test_delete_data_by_date_invalid_table(mock_get_table_info):
    """Test handling of invalid table name."""
    # Mock table info to raise exception
    mock_get_table_info.side_effect = AccessDatabaseError("Table not found: nonexistent_table")
    
    # Setup test data
    test_date = date(2023, 1, 5)
    date_filter = DateFilter(
        start_date=test_date,
        end_date=test_date,
        is_full_date=True
    )
    
    # Test should raise exception
    with pytest.raises(AccessDatabaseError) as exc_info:
        delete_data_by_date(
            db_path=Path("dummy.accdb"),
            table_name="nonexistent_table",
            date_filter=date_filter,
            date_column="date"
        )
    
    assert "Table not found" in str(exc_info.value) 