"""
Unit tests for database operations.
This module contains tests for the DatabaseOperations class and its methods.
"""

import pytest
from pathlib import Path
from datasync.database.operations import DatabaseOperations
from datasync.database.validation import DatabaseValidation
from datasync.database.monitoring import DatabaseMonitor

@pytest.fixture
def mock_db_path(tmp_path):
    """Create a temporary database path for testing."""
    return tmp_path / "test_database.accdb"

@pytest.fixture
def db_operations(mock_db_path):
    """Create a DatabaseOperations instance with a mock database path."""
    return DatabaseOperations(mock_db_path)

@pytest.fixture
def db_validation():
    """Create a DatabaseValidation instance."""
    return DatabaseValidation()

@pytest.fixture
def db_monitor():
    """Create a DatabaseMonitor instance."""
    return DatabaseMonitor()

class TestDatabaseOperations:
    """Test suite for DatabaseOperations class."""
    
    def test_initialization(self, db_operations, mock_db_path):
        """Test database operations initialization."""
        assert db_operations.db_path == mock_db_path
        assert db_operations.conn is None
        assert db_operations.cursor is None
    
    def test_connection_string(self, db_operations):
        """Test connection string generation."""
        conn_str = db_operations.conn_str
        assert "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}" in conn_str
        assert str(db_operations.db_path.absolute()) in conn_str
    
    def test_get_tables(self, db_operations, mocker):
        """Test getting list of tables."""
        # Mock the cursor and its tables method
        mock_cursor = mocker.MagicMock()
        mock_cursor.tables.return_value = [
            mocker.MagicMock(table_name="Table1", table_type="TABLE"),
            mocker.MagicMock(table_name="Table2", table_type="VIEW"),
            mocker.MagicMock(table_name="Table3", table_type="TABLE")
        ]
        db_operations.cursor = mock_cursor
        
        tables = db_operations.get_tables()
        assert len(tables) == 2
        assert "Table1" in tables
        assert "Table3" in tables
    
    def test_execute_query_select(self, db_operations, mocker):
        """Test executing a SELECT query."""
        # Mock the cursor and its methods
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1"), (2, "Test2")]
        db_operations.cursor = mock_cursor
        
        results = db_operations.execute_query("SELECT * FROM test_table")
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[0]["name"] == "Test1"
    
    def test_execute_query_insert(self, db_operations, mocker):
        """Test executing an INSERT query."""
        # Mock the cursor and its methods
        mock_cursor = mocker.MagicMock()
        mock_cursor.rowcount = 1
        db_operations.cursor = mock_cursor
        db_operations.conn = mocker.MagicMock()
        
        affected_rows = db_operations.execute_query(
            "INSERT INTO test_table (name) VALUES (?)",
            ("Test",)
        )
        assert affected_rows == 1
    
    def test_get_table_columns(self, db_operations, mocker):
        """Test getting table columns."""
        # Mock the cursor and its description
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",), ("value",)]
        db_operations.cursor = mock_cursor
        
        columns = db_operations.get_table_columns("test_table")
        assert len(columns) == 3
        assert "id" in columns
        assert "name" in columns
        assert "value" in columns
    
    def test_count_records(self, db_operations, mocker):
        """Test counting records for a specific year."""
        # Mock execute_query to return a count
        mocker.patch.object(
            db_operations,
            'execute_query',
            return_value=[{'record_count': 42}]
        )
        
        count = db_operations.count_records("test_table", 2023)
        assert count == 42
    
    def test_delete_year_data(self, db_operations, mocker):
        """Test deleting data for a specific year."""
        # Mock count_records and execute_query
        mocker.patch.object(
            db_operations,
            'count_records',
            return_value=10
        )
        mocker.patch.object(
            db_operations,
            'execute_query',
            return_value=10
        )
        
        deleted = db_operations.delete_year_data("test_table", 2023)
        assert deleted == 10

    def test_insert_record(self, db_operations, mocker):
        """Test inserting a single record."""
        # Mock the cursor and its methods
        mock_cursor = mocker.MagicMock()
        mock_cursor.rowcount = 1
        db_operations.cursor = mock_cursor
        db_operations.conn = mocker.MagicMock()
        
        # Test data
        record = {
            "id": 1,
            "name": "Test Record",
            "value": 42
        }
        
        result = db_operations.insert_record("test_table", record)
        assert result == 1
        mock_cursor.execute.assert_called_once()
        db_operations.conn.commit.assert_called_once()

    def test_insert_record_error(self, db_operations, mocker):
        """Test error handling in insert_record."""
        # Mock the cursor to raise an error
        mock_cursor = mocker.MagicMock()
        mock_cursor.execute.side_effect = Exception("Test error")
        db_operations.cursor = mock_cursor
        db_operations.conn = mocker.MagicMock()
        
        with pytest.raises(Exception):
            db_operations.insert_record("test_table", {"id": 1})
        db_operations.conn.rollback.assert_called_once()

    def test_batch_insert(self, db_operations, mocker):
        """Test batch insert functionality."""
        # Mock necessary methods
        mock_cursor = mocker.MagicMock()
        db_operations.cursor = mock_cursor
        db_operations.conn = mocker.MagicMock()
        mocker.patch.object(
            db_operations,
            'execute_query',
            return_value=1
        )
        
        # Test data
        records = [
            {"id": 1, "name": "Test1"},
            {"id": 2, "name": "Test2"},
            {"id": 3, "name": "Test3"}
        ]
        
        result = db_operations.batch_insert("test_table", records, batch_size=2)
        assert result == 3
        assert mock_cursor.executemany.call_count == 2  # Two batches of 2 and 1 records

    def test_batch_insert_empty(self, db_operations):
        """Test batch insert with empty records list."""
        result = db_operations.batch_insert("test_table", [])
        assert result == 0

    def test_upsert_insert(self, db_operations, mocker):
        """Test upsert when record doesn't exist (insert case)."""
        # Mock execute_query to return no existing records
        mocker.patch.object(
            db_operations,
            'execute_query',
            return_value=[{'count': 0}]
        )
        # Mock insert_record
        mocker.patch.object(
            db_operations,
            'insert_record',
            return_value=1
        )
        
        record = {"id": 1, "name": "Test"}
        result = db_operations.upsert("test_table", record, ["id"])
        assert result == 1
        db_operations.insert_record.assert_called_once_with("test_table", record)

    def test_upsert_update(self, db_operations, mocker):
        """Test upsert when record exists (update case)."""
        # Mock execute_query to return existing record
        mocker.patch.object(
            db_operations,
            'execute_query',
            return_value=[{'count': 1}]
        )
        
        # Mock cursor
        mock_cursor = mocker.MagicMock()
        mock_cursor.rowcount = 1
        db_operations.cursor = mock_cursor
        db_operations.conn = mocker.MagicMock()
        
        record = {"id": 1, "name": "Updated"}
        result = db_operations.upsert("test_table", record, ["id"])
        assert result == 1
        mock_cursor.execute.assert_called_once()
        db_operations.conn.commit.assert_called_once()

    def test_transaction_management(self, db_operations, mocker):
        """Test transaction management methods."""
        # Mock connection
        mock_conn = mocker.MagicMock()
        db_operations.conn = mock_conn
        
        # Test begin_transaction
        db_operations.begin_transaction()
        assert mock_conn.autocommit is False
        
        # Test commit_transaction
        db_operations.commit_transaction()
        mock_conn.commit.assert_called_once()
        assert mock_conn.autocommit is True
        
        # Test rollback_transaction
        db_operations.rollback_transaction()
        mock_conn.rollback.assert_called_once()
        assert mock_conn.autocommit is True

    def test_transaction_error_handling(self, db_operations, mocker):
        """Test transaction error handling."""
        # Mock connection to raise error
        mock_conn = mocker.MagicMock()
        mock_conn.commit.side_effect = Exception("Commit error")
        db_operations.conn = mock_conn
        
        with pytest.raises(Exception):
            db_operations.commit_transaction()
        assert mock_conn.autocommit is True 