"""
Unit tests for database operations.
This module contains tests for the DatabaseOperations class and its methods.
"""

import pytest
from pathlib import Path
from datasync.database.operations import DatabaseOperations
from datasync.database.validation import DatabaseValidation
from datasync.database.monitoring import DatabaseMonitor
from datasync.utils.path_utils import normalize_path
import pyodbc
from tests.fixtures.database import (
    mock_db_path,
    db_operations,
    db_validation,
    db_monitor,
    setup_test_table,
    setup_yearly_data,
    setup_transaction_test
)
from tests.fixtures.mock_database.create_mock_db import create_mock_database
from datetime import datetime
from datasync.database.sql_syntax import AccessSQLSyntax
import os
from unittest.mock import Mock, patch

@pytest.fixture
def mock_db_path(tmp_path):
    """Create a temporary database file for testing."""
    # Ensure the directory exists
    tmp_path.mkdir(parents=True, exist_ok=True)
    
    # Create the database file
    db_path = tmp_path / "test_database.accdb"
    db_path.touch()  # Create an empty file
    
    return str(db_path)

@pytest.fixture
def db_operations(mock_db_path):
    """Initialize database operations with mock database."""
    # Create a new database file if it doesn't exist
    if not os.path.exists(mock_db_path):
        with open(mock_db_path, 'w') as f:
            pass  # Create empty file
            
    db = DatabaseOperations(mock_db_path)
    db.connect()
    yield db
    try:
        db.close()
    except Exception:
        pass

@pytest.fixture
def setup_test_table(db_operations):
    """Create a test table with sample data."""
    try:
        # Create test table
        db_operations.create_table(
            "test_table",
            {
                "id": "LONG",
                "name": "VARCHAR(255)",
                "value": "DOUBLE",
                "category": "VARCHAR(50)",
                "date_field": "DATE"
            },
            primary_key=["id"]
        )
        
        # Insert test data
        test_data = [
            {"id": 1, "name": "Test 1", "value": 10.5, "category": "A", "date_field": datetime(2023, 1, 1)},
            {"id": 2, "name": "Test 2", "value": 20.0, "category": "A", "date_field": datetime(2023, 6, 1)},
            {"id": 3, "name": "Test 3", "value": 30.0, "category": "B", "date_field": datetime(2024, 1, 1)}
        ]
        
        for record in test_data:
            db_operations.insert_record("test_table", record)
            
        yield "test_table"
        
    finally:
        try:
            db_operations.execute_query("DROP TABLE [test_table]")
        except Exception:
            pass

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
    
    @pytest.fixture(autouse=True)
    def setup_method(self, db_operations):
        """Set up test environment before each test method."""
        self.db = db_operations
        self.test_table_name = "test_table"
        
        # Create test table using DatabaseOperations methods
        try:
            self.db.create_table(
                self.test_table_name,
                {
                    "id": "COUNTER",
                    "date_field": "DATETIME",
                    "value": "DOUBLE",
                    "category": "TEXT(50)"
                },
                primary_key=["id"]
            )
            
            # Insert some test data
            test_data = [
                {"date_field": datetime(2023, 1, 1), "value": 10.5, "category": "A"},
                {"date_field": datetime(2023, 6, 1), "value": 20.0, "category": "A"},
                {"date_field": datetime(2024, 1, 1), "value": 30.0, "category": "B"}
            ]
            
            for record in test_data:
                self.db.insert_record(self.test_table_name, record)
                
            self.db.commit_transaction()
            
        except Exception as e:
            if "already exists" not in str(e):
                if self.db.in_transaction:
                    self.db.rollback_transaction()
                raise
            self.db.rollback_transaction()

    def teardown_method(self):
        """Clean up after each test method."""
        try:
            # Drop test table if it exists
            self.db.execute_query(f"DROP TABLE [{self.test_table_name}]")
            self.db.commit_transaction()
        except Exception:
            pass  # Ignore cleanup errors
            
        if self.db.in_transaction:
            self.db.rollback_transaction()

    def test_connection_management(self, db_operations):
        """Test database connection management."""
        assert db_operations.conn is not None
        assert db_operations.cursor is not None
        
        db_operations.close()
        assert db_operations.conn is None
        assert db_operations.cursor is None
        
        db_operations.connect()
        assert db_operations.conn is not None
        assert db_operations.cursor is not None

    def test_table_operations(self, db_operations):
        """Test basic table operations."""
        # Create test table
        db_operations.create_table(
            "test_table",
            {
                "id": "LONG",
                "name": "VARCHAR(255)",
                "value": "DOUBLE"
            },
            primary_key=["id"]
        )
        
        # Verify table exists
        tables = db_operations.get_tables()
        assert "test_table" in tables
        
        # Get columns
        columns = db_operations.get_table_columns("test_table")
        assert "id" in columns
        assert "name" in columns
        assert "value" in columns
        
        # Clean up
        db_operations.execute_query("DROP TABLE [test_table]")

    def test_insert_operations(self, db_operations, setup_test_table):
        """Test insert operations."""
        table_name = setup_test_table
        
        # Test single insert
        record = {
            "id": 4,
            "name": "Test 4",
            "value": 40.0,
            "category": "B",
            "date_field": datetime(2024, 6, 1)
        }
        result = db_operations.insert_record(table_name, record)
        assert result == 1
        
        # Verify insert
        query = f"SELECT * FROM [{table_name}] WHERE id = 4"
        result = db_operations.execute_query(query)
        assert len(result) == 1
        assert result[0]["name"] == "Test 4"

    def test_update_operations(self, db_operations, setup_test_table):
        """Test update operations."""
        table_name = setup_test_table
        
        # Update record
        update_data = {
            "id": 1,
            "name": "Updated Test 1",
            "value": 15.5
        }
        result = db_operations.update_record(table_name, update_data, ["id"])
        assert result == 1
        
        # Verify update
        query = f"SELECT * FROM [{table_name}] WHERE id = 1"
        result = db_operations.execute_query(query)
        assert len(result) == 1
        assert result[0]["name"] == "Updated Test 1"
        assert result[0]["value"] == 15.5

    def test_delete_operations(self, db_operations, setup_test_table):
        """Test delete operations."""
        table_name = setup_test_table
        
        # Delete record
        result = db_operations.delete_records(table_name, {"id": 1})
        assert result == 1
        
        # Verify delete
        query = f"SELECT * FROM [{table_name}] WHERE id = 1"
        result = db_operations.execute_query(query)
        assert len(result) == 0

    def test_transaction_operations(self, db_operations, setup_test_table):
        """Test transaction operations."""
        table_name = setup_test_table
        
        # Start transaction
        db_operations.begin_transaction()
        assert db_operations.in_transaction is True
        
        # Insert record in transaction
        record = {
            "id": 4,
            "name": "Transaction Test",
            "value": 40.0,
            "category": "C",
            "date_field": datetime(2024, 1, 1)
        }
        db_operations.insert_record(table_name, record)
        
        # Verify record exists
        query = f"SELECT * FROM [{table_name}] WHERE id = 4"
        result = db_operations.execute_query(query)
        assert len(result) == 1
        
        # Rollback transaction
        db_operations.rollback_transaction()
        assert db_operations.in_transaction is False
        
        # Verify record was rolled back
        result = db_operations.execute_query(query)
        assert len(result) == 0

    def test_initialization(self, db_operations, mock_db_path):
        """Test database operations initialization."""
        assert db_operations.db_path == normalize_path(mock_db_path)
        assert db_operations.conn is None
        assert db_operations.cursor is None
    
    def test_connection_string(self, db_operations, tmp_path):
        """Test connection string generation."""
        # Test with absolute path
        test_db = tmp_path / "database.accdb"
        db_operations.db_path = test_db
        conn_str = db_operations.conn_str
        
        # Basic connection string format checks
        assert "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}" in conn_str
        assert "DBQ=" in conn_str
        
        # Path format checks
        db_path_part = conn_str.split("DBQ=")[1]
        assert "\\\\" in db_path_part  # Should have double backslashes
        expected_path = str(test_db.absolute()).replace("\\", "\\\\")
        assert expected_path == db_path_part
        
        # Test with relative path
        relative_db = Path("database.accdb")
        db_operations.db_path = relative_db
        conn_str = db_operations.conn_str
        assert "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}" in conn_str
        assert "DBQ=" in conn_str
        expected_path = str(relative_db.absolute()).replace("\\", "\\\\")
        assert expected_path == conn_str.split("DBQ=")[1]
    
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
        # Create test table
        columns = {
            'id': 'COUNTER',
            'name': 'TEXT(50)'
        }
        db_operations.create_table('test_table', columns, ['id'])
        
        # Insert test data
        db_operations.execute_query(
            "INSERT INTO test_table (name) VALUES (?)",
            ("Test1",)
        )
        
        # Mock the cursor and its methods
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1"), (2, "Test2")]
        db_operations.cursor = mock_cursor
        
        results = db_operations.execute_query("SELECT * FROM test_table")
        assert len(results) == 2
        assert results[0]['id'] == 1
        assert results[0]['name'] == "Test1"
        
        # Cleanup
        db_operations.execute_query("DROP TABLE test_table")
    
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
    
    def test_count_records(self):
        """Test counting records with filters."""
        # Insert test data
        insert_sql = """
        INSERT INTO [test_table] ([date_field], [value], [category])
        VALUES (?, ?, ?)
        """
        test_data = [
            (datetime(2023, 1, 1), 10.0, 'A'),
            (datetime(2023, 6, 15), 20.0, 'A'),
            (datetime(2024, 1, 1), 30.0, 'B')
        ]
        for record in test_data:
            self.db.execute_query(insert_sql, record)
        self.db.commit_transaction()
        
        # Test count with year filter
        count = self.db.count_records(self.test_table_name, year=2023)
        assert count == 2
        
        # Test count with category filter
        count = self.db.count_records(self.test_table_name, filters={'category': 'A'})
        assert count == 2

    def test_delete_year_data(self):
        """Test deleting records for a specific year."""
        # Insert test data
        insert_sql = """
        INSERT INTO [test_table] ([date_field], [value], [category])
        VALUES (?, ?, ?)
        """
        test_data = [
            (datetime(2023, 1, 1), 10.0, 'A'),
            (datetime(2023, 6, 15), 20.0, 'B'),
            (datetime(2024, 1, 1), 30.0, 'A')
        ]
        for record in test_data:
            self.db.execute_query(insert_sql, record)
        self.db.commit_transaction()
        
        # Test deletion
        self.db.delete_year_data(self.test_table_name, 2023)
        
        # Verify deletion
        count_sql = "SELECT COUNT(*) FROM [test_table] WHERE YEAR([date_field]) = ?"
        count = self.db.execute_query_scalar(count_sql, (2023,))
        assert count == 0
        
        # Verify 2024 records remain
        count = self.db.execute_query_scalar(count_sql, (2024,))
        assert count == 1

    def test_read_records_with_filters(self):
        """Test reading records with filters."""
        # Insert test data
        insert_sql = """
        INSERT INTO [test_table] ([date_field], [value], [category])
        VALUES (?, ?, ?)
        """
        test_data = [
            (datetime(2023, 1, 1), 10.0, 'A'),
            (datetime(2023, 6, 15), 20.0, 'A'),
            (datetime(2023, 12, 31), 30.0, 'B')
        ]
        for record in test_data:
            self.db.execute_query(insert_sql, record)
        self.db.commit_transaction()
        
        # Test reading with filters
        filters = {
            'category': 'A',
            'value': {'min': 15.0, 'max': 25.0}
        }
        records = self.db.read_records(self.test_table_name, filters=filters)
        
        assert len(records) == 1
        assert records[0]['value'] == 20.0
        assert records[0]['category'] == 'A'

    def test_read_records_with_sorting(self, db_operations, mocker):
        """Test read_records with sorting."""
        # Create test table
        columns = {
            'id': 'COUNTER',
            'name': 'TEXT(50)',
            'value': 'DOUBLE',
            'date_field': 'DATETIME'
        }
        db_operations.create_table('test_table', columns, ['id'])
        
        try:
            # Insert test data
            test_data = [
                {'name': 'Test1', 'value': 10.0, 'date_field': '2024-01-01'},
                {'name': 'Test2', 'value': 20.0, 'date_field': '2024-01-02'},
                {'name': 'Test3', 'value': 30.0, 'date_field': '2024-01-03'}
            ]
            
            for record in test_data:
                db_operations.insert_record('test_table', record)
            
            # Mock the cursor and its methods
            mock_cursor = mocker.MagicMock()
            mock_cursor.description = [("id",), ("name",), ("value",), ("date_field",)]
            mock_cursor.fetchall.return_value = [
                (3, "Test3", 30.0, "2024-01-03"),
                (2, "Test2", 20.0, "2024-01-02"),
                (1, "Test1", 10.0, "2024-01-01")
            ]
            db_operations.cursor = mock_cursor
            
            # Test descending sort
            results = db_operations.read_records("test_table", sort_by="value", sort_order="DESC")
            
            assert len(results) == 3
            assert results[0]['value'] == 30.0
            assert results[1]['value'] == 20.0
            assert results[2]['value'] == 10.0
            
            # Test ascending sort
            mock_cursor.fetchall.return_value = [
                (1, "Test1", 10.0, "2024-01-01"),
                (2, "Test2", 20.0, "2024-01-02"),
                (3, "Test3", 30.0, "2024-01-03")
            ]
            results = db_operations.read_records("test_table", sort_by="value", sort_order="ASC")
            
            assert len(results) == 3
            assert results[0]['value'] == 10.0
            assert results[1]['value'] == 20.0
            assert results[2]['value'] == 30.0
            
        finally:
            # Clean up
            if db_operations.conn:
                db_operations.execute_query('DROP TABLE test_table')
                db_operations.close()

    def test_read_records_with_pagination(self, db_operations, mocker):
        """Test read_records with pagination."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(3, "Test3"), (4, "Test4")]
        db_operations.cursor = mock_cursor
        
        results = db_operations.read_records("test_table", limit=2, offset=2)
        assert len(results) == 2
        assert results[0]["id"] == 3
        
    def test_build_query_basic(self, db_operations):
        """Test basic query building."""
        query = db_operations.build_query("test_table")
        assert query == "SELECT * FROM [test_table]"
        
    def test_build_query_with_columns(self, db_operations):
        """Test query building with specific columns."""
        query = db_operations.build_query("test_table", columns=["id", "name"])
        assert query == "SELECT [id], [name] FROM [test_table]"
        
    def test_build_query_with_joins(self, db_operations):
        """Test query building with joins."""
        joins = [{"table": "other_table", "on": "test_table.id = other_table.test_id"}]
        query = db_operations.build_query("test_table", joins=joins)
        assert "INNER JOIN [other_table] ON test_table.id = other_table.test_id" in query
        
    def test_build_query_with_filters(self, db_operations):
        """Test query building with filters."""
        filters = {"name": "Test1", "status": "active"}
        query = db_operations.build_query("test_table", filters=filters)
        assert "[name] = ?" in query
        assert "[status] = ?" in query
        assert "WHERE" in query
        
    def test_build_query_with_group_by(self, db_operations):
        """Test query building with GROUP BY."""
        query = db_operations.build_query("test_table", group_by=["status"])
        assert "GROUP BY [status]" in query
        
    def test_build_query_with_having(self, db_operations):
        """Test query building with HAVING clause."""
        having = {"count": 5}
        query = db_operations.build_query("test_table", having=having)
        assert "HAVING [count] = ?" in query
        
    def test_build_query_with_sorting(self, db_operations):
        """Test query building with sorting."""
        query = db_operations.build_query("test_table", sort_by=["name"], sort_desc=True)
        assert "ORDER BY [name] DESC" in query
        
    def test_build_query_with_pagination(self, db_operations):
        """Test query building with pagination."""
        query = db_operations.build_query("test_table", limit=10, offset=20)
        assert "LIMIT 10 OFFSET 20" in query

    def test_read_records_with_in_clause(self, db_operations, mocker):
        """Test read_records with IN clause."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1"), (2, "Test2")]
        db_operations.cursor = mock_cursor
        
        filters = {"id": [1, 2, 3]}
        results = db_operations.read_records("test_table", filters=filters)
        assert len(results) == 2
        assert results[0]["id"] in [1, 2]
        
    def test_read_records_with_date_range(self, db_operations, mocker):
        """Test read_records with date range filtering."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("date",)]
        mock_cursor.fetchall.return_value = [(1, "2023-01-01"), (2, "2023-01-15")]
        db_operations.cursor = mock_cursor
        
        date_range = {
            "column": "date",
            "start_date": "2023-01-01",
            "end_date": "2023-01-31"
        }
        results = db_operations.read_records("test_table", date_range=date_range)
        assert len(results) == 2
        assert results[0]["date"] == "2023-01-01"
        
    def test_read_records_with_custom_filters(self, db_operations, mocker):
        """Test read_records with custom SQL filters."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1")]
        db_operations.cursor = mock_cursor
        
        custom_filters = ["LENGTH([name]) > 4", "[id] % 2 = 1"]
        results = db_operations.read_records("test_table", custom_filters=custom_filters)
        assert len(results) == 1
        assert results[0]["id"] == 1
        
    def test_aggregate_query_basic(self, db_operations, mocker):
        """Test basic aggregate query."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("total_amount",), ("count",)]
        mock_cursor.fetchall.return_value = [(1000, 5)]
        db_operations.cursor = mock_cursor
        
        aggregates = {
            "total_amount": "SUM(amount)",
            "count": "COUNT(*)"
        }
        results = db_operations.aggregate_query("test_table", aggregates)
        assert len(results) == 1
        assert results[0]["total_amount"] == 1000
        assert results[0]["count"] == 5
        
    def test_aggregate_query_with_grouping(self, db_operations, mocker):
        """Test aggregate query with grouping."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("category",), ("total_amount",)]
        mock_cursor.fetchall.return_value = [("A", 500), ("B", 300)]
        db_operations.cursor = mock_cursor
        
        aggregates = {"total_amount": "SUM(amount)"}
        group_by = ["category"]
        results = db_operations.aggregate_query("test_table", aggregates, group_by)
        assert len(results) == 2
        assert results[0]["category"] == "A"
        assert results[0]["total_amount"] == 500
        
    def test_aggregate_query_with_having(self, db_operations, mocker):
        """Test aggregate query with HAVING clause."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("category",), ("total_amount",)]
        mock_cursor.fetchall.return_value = [("A", 1000)]
        db_operations.cursor = mock_cursor
        
        aggregates = {"total_amount": "SUM(amount)"}
        group_by = ["category"]
        having = {"total_amount": 1000}
        results = db_operations.aggregate_query("test_table", aggregates, group_by, having=having)
        assert len(results) == 1
        assert results[0]["total_amount"] == 1000
        
    def test_subquery_basic(self, db_operations, mocker):
        """Test basic subquery functionality."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1")]
        db_operations.cursor = mock_cursor
        
        subquery = "SELECT id FROM other_table WHERE status = ?"
        subquery_params = ("active",)
        results = db_operations.subquery("test_table", subquery, subquery_params)
        assert len(results) == 1
        assert results[0]["id"] == 1
        
    def test_subquery_with_filters(self, db_operations, mocker):
        """Test subquery with additional filters."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1")]
        db_operations.cursor = mock_cursor
        
        subquery = "SELECT id FROM other_table WHERE status = ?"
        subquery_params = ("active",)
        filters = {"name": "Test1"}
        results = db_operations.subquery("test_table", subquery, subquery_params, filters)
        assert len(results) == 1
        assert results[0]["name"] == "Test1"

    def test_update_record(self, db_operations):
        """Test updating a single record."""
        # Create test table
        db_operations.execute_query("""
            CREATE TABLE test_update (
                id INTEGER PRIMARY KEY,
                name TEXT(50),
                value DECIMAL(10,2),
                status TEXT(20)
            )
        """)
        
        try:
            # Insert test record
            db_operations.execute_query(
                "INSERT INTO test_update (id, name, value, status) VALUES (?, ?, ?, ?)",
                (1, "Test", 10.5, "active")
            )
            
            # Update record
            record = {
                "id": 1,
                "name": "Updated",
                "value": 20.75,
                "status": "inactive"
            }
            result = db_operations.update_record("test_update", record, ["id"])
            assert result == 1
            
            # Verify update
            results = db_operations.execute_query("SELECT * FROM test_update WHERE id = 1")
            assert len(results) == 1
            assert results[0]["name"] == "Updated"
            assert results[0]["value"] == 20.75
            assert results[0]["status"] == "inactive"
            
        finally:
            db_operations.execute_query("DROP TABLE test_update")

    def test_update_record_nonexistent(self, db_operations):
        """Test updating a non-existent record."""
        # Create test table
        db_operations.execute_query("""
            CREATE TABLE test_update_nonexistent (
                id INTEGER PRIMARY KEY,
                name TEXT(50)
            )
        """)
        
        try:
            # Try to update non-existent record
            record = {"id": 1, "name": "Test"}
            result = db_operations.update_record("test_update_nonexistent", record, ["id"])
            assert result == 0
            
        finally:
            db_operations.execute_query("DROP TABLE test_update_nonexistent")

    def test_batch_update(self, db_operations):
        """Test batch updating multiple records."""
        # Create test table
        db_operations.execute_query("""
            CREATE TABLE test_batch_update (
                id INTEGER PRIMARY KEY,
                name TEXT(50),
                value DECIMAL(10,2)
            )
        """)
        
        try:
            # Insert test records
            for i in range(1, 6):
                db_operations.execute_query(
                    "INSERT INTO test_batch_update (id, name, value) VALUES (?, ?, ?)",
                    (i, f"Test {i}", float(i) * 10)
                )
            
            # Prepare update records
            records = [
                {"id": 1, "name": "Updated 1", "value": 100.0},
                {"id": 2, "name": "Updated 2", "value": 200.0},
                {"id": 3, "name": "Updated 3", "value": 300.0}
            ]
            
            # Update records in batch
            result = db_operations.batch_update("test_batch_update", records, ["id"], batch_size=2)
            assert result == 3
            
            # Verify updates
            results = db_operations.execute_query("SELECT * FROM test_batch_update ORDER BY id")
            assert len(results) == 5
            assert results[0]["name"] == "Updated 1"
            assert results[0]["value"] == 100.0
            assert results[1]["name"] == "Updated 2"
            assert results[1]["value"] == 200.0
            assert results[2]["name"] == "Updated 3"
            assert results[2]["value"] == 300.0
            
        finally:
            db_operations.execute_query("DROP TABLE test_batch_update")

    def test_batch_update_empty(self, db_operations):
        """Test batch update with empty records list."""
        # Create test table
        db_operations.execute_query("""
            CREATE TABLE test_batch_update_empty (
                id INTEGER PRIMARY KEY,
                name TEXT(50)
            )
        """)
        
        try:
            # Try batch update with empty list
            result = db_operations.batch_update("test_batch_update_empty", [], ["id"])
            assert result == 0
            
        finally:
            db_operations.execute_query("DROP TABLE test_batch_update_empty")

    def test_update_with_conditions(self, db_operations):
        """Test updating records with conditions."""
        # Create test table
        db_operations.execute_query("""
            CREATE TABLE test_update_conditions (
                id INTEGER PRIMARY KEY,
                category TEXT(50),
                status TEXT(20),
                value DECIMAL(10,2)
            )
        """)
        
        try:
            # Insert test records
            test_data = [
                (1, "A", "active", 10.0),
                (2, "A", "inactive", 20.0),
                (3, "B", "active", 30.0),
                (4, "B", "inactive", 40.0)
            ]
            
            for record in test_data:
                db_operations.execute_query(
                    "INSERT INTO test_update_conditions VALUES (?, ?, ?, ?)",
                    record
                )
            
            # Update records with conditions
            updates = {"status": "completed", "value": 50.0}
            conditions = {"category": "A"}
            result = db_operations.update_with_conditions("test_update_conditions", updates, conditions)
            assert result == 2
            
            # Verify updates
            results = db_operations.execute_query(
                "SELECT * FROM test_update_conditions WHERE category = 'A'"
            )
            assert len(results) == 2
            assert all(r["status"] == "completed" for r in results)
            assert all(r["value"] == 50.0 for r in results)
            
        finally:
            db_operations.execute_query("DROP TABLE test_update_conditions")

    def test_update_with_conditions_no_match(self, db_operations):
        """Test updating records with conditions that match no records."""
        # Create test table
        db_operations.execute_query("""
            CREATE TABLE test_update_conditions_no_match (
                id INTEGER PRIMARY KEY,
                category TEXT(50)
            )
        """)
        
        try:
            # Insert test record
            db_operations.execute_query(
                "INSERT INTO test_update_conditions_no_match VALUES (?, ?)",
                (1, "A")
            )
            
            # Try to update with non-matching condition
            updates = {"category": "B"}
            conditions = {"category": "C"}
            result = db_operations.update_with_conditions(
                "test_update_conditions_no_match", 
                updates, 
                conditions
            )
            assert result == 0
            
        finally:
            db_operations.execute_query("DROP TABLE test_update_conditions_no_match")

    def test_update_transaction_rollback(self, db_operations):
        """Test transaction rollback during update."""
        # Create test table
        db_operations.execute_query("""
            CREATE TABLE test_update_transaction (
                id INTEGER PRIMARY KEY,
                name TEXT(50) NOT NULL,
                value DECIMAL(10,2)
            )
        """)
        
        try:
            # Insert test record
            db_operations.execute_query(
                "INSERT INTO test_update_transaction VALUES (?, ?, ?)",
                (1, "Test", 10.0)
            )
            
            # Start transaction
            db_operations.begin_transaction()
            
            # Update record
            record = {"id": 1, "name": "Updated", "value": 20.0}
            db_operations.update_record("test_update_transaction", record, ["id"])
            
            # Try to update with invalid data (should fail)
            with pytest.raises(pyodbc.Error):
                invalid_record = {"id": 1, "name": None, "value": 30.0}
                db_operations.update_record("test_update_transaction", invalid_record, ["id"])
            
            # Verify rollback
            results = db_operations.execute_query("SELECT * FROM test_update_transaction")
            assert len(results) == 1
            assert results[0]["name"] == "Test"
            assert results[0]["value"] == 10.0
            
        finally:
            db_operations.execute_query("DROP TABLE test_update_transaction")

    def test_delete_records(self, db_operations, mock_db_path):
        """Test deleting records from a table."""
        # Create test table
        table_name = "test_delete_table"
        self.db.create_table(
            table_name,
            {
                "id": "COUNTER",
                "name": "TEXT(50)",
                "value": "DOUBLE"
            },
            primary_key=["id"]
        )
        
        # Insert test data
        test_data = [
            {"name": "Test 1", "value": 10.5},
            {"name": "Test 2", "value": 20.0},
            {"name": "Test 3", "value": 30.0}
        ]
        
        for record in test_data:
            self.db.insert_record(table_name, record)
            
        # Delete records with condition
        conditions = {"name": "Test 2"}
        self.db.delete_records(table_name, conditions)
        
        # Verify deletion
        remaining_records = self.db.read_records(table_name)
        assert len(remaining_records) == 2
        assert all(record["name"] != "Test 2" for record in remaining_records)
        
        # Cleanup
        self.db.drop_table(table_name)

    def test_soft_delete(self, db_operations, mock_db_path):
        """Test soft delete functionality."""
        # Create test table with is_deleted column
        table_name = "test_soft_delete_table"
        self.db.create_table(
            table_name,
            {
                "id": "COUNTER",
                "name": "TEXT(50)",
                "value": "DOUBLE",
                "is_deleted": "BIT"
            },
            primary_key=["id"]
        )
        
        # Insert test data
        test_data = [
            {"name": "Test 1", "value": 10.5, "is_deleted": False},
            {"name": "Test 2", "value": 20.0, "is_deleted": False},
            {"name": "Test 3", "value": 30.0, "is_deleted": False}
        ]
        
        for record in test_data:
            self.db.insert_record(table_name, record)
            
        # Soft delete a record
        conditions = {"name": "Test 2"}
        self.db.update_record(table_name, {"is_deleted": True}, conditions)
        
        # Verify soft delete
        remaining_records = self.db.read_records(table_name, {"is_deleted": False})
        assert len(remaining_records) == 2
        assert all(record["name"] != "Test 2" for record in remaining_records)
        
        # Cleanup
        self.db.drop_table(table_name)

    def test_cascade_delete(self, db_operations, mock_db_path):
        """Test cascade delete functionality."""
        # Create parent and child tables
        parent_table = "test_parent_table"
        child_table = "test_child_table"
        
        self.db.create_table(
            parent_table,
            {
                "id": "COUNTER",
                "name": "TEXT(50)"
            },
            primary_key=["id"]
        )
        
        self.db.create_table(
            child_table,
            {
                "id": "COUNTER",
                "parent_id": "LONG",
                "value": "DOUBLE"
            },
            primary_key=["id"]
        )
        
        # Add foreign key constraint
        self.db.add_foreign_key(child_table, "parent_id", parent_table, "id")
        
        # Insert test data
        parent_data = [
            {"name": "Parent 1"},
            {"name": "Parent 2"}
        ]
        
        for record in parent_data:
            self.db.insert_record(parent_table, record)
            
        child_data = [
            {"parent_id": 1, "value": 10.5},
            {"parent_id": 1, "value": 20.0},
            {"parent_id": 2, "value": 30.0}
        ]
        
        for record in child_data:
            self.db.insert_record(child_table, record)
            
        # Delete parent record (should cascade to child records)
        self.db.delete_records(parent_table, {"id": 1})
        
        # Verify cascade delete
        remaining_parents = self.db.read_records(parent_table)
        remaining_children = self.db.read_records(child_table)
        
        assert len(remaining_parents) == 1
        assert len(remaining_children) == 1
        assert remaining_children[0]["parent_id"] == 2
        
        # Cleanup
        self.db.drop_table(child_table)
        self.db.drop_table(parent_table)

    def test_delete_with_transaction(self, db_operations, mock_db_path):
        """Test delete operations within a transaction."""
        # Create test table
        table_name = "test_transaction_delete_table"
        self.db.create_table(
            table_name,
            {
                "id": "COUNTER",
                "name": "TEXT(50)",
                "value": "DOUBLE"
            },
            primary_key=["id"]
        )
        
        # Insert test data
        test_data = [
            {"name": "Test 1", "value": 10.5},
            {"name": "Test 2", "value": 20.0},
            {"name": "Test 3", "value": 30.0}
        ]
        
        for record in test_data:
            self.db.insert_record(table_name, record)
            
        # Start transaction
        self.db.begin_transaction()
        
        try:
            # Delete records
            conditions = {"name": "Test 2"}
            self.db.delete_records(table_name, conditions)
            
            # Verify deletion within transaction
            remaining_records = self.db.read_records(table_name)
            assert len(remaining_records) == 2
            
            # Commit transaction
            self.db.commit_transaction()
            
        except Exception:
            self.db.rollback_transaction()
            raise
            
        # Verify deletion after commit
        remaining_records = self.db.read_records(table_name)
        assert len(remaining_records) == 2
        assert all(record["name"] != "Test 2" for record in remaining_records)
        
        # Cleanup
        self.db.drop_table(table_name)

    def test_delete_transaction_rollback(self, db_operations, mock_db_path):
        """Test rollback of delete operations."""
        # Create test table
        table_name = "test_rollback_delete_table"
        self.db.create_table(
            table_name,
            {
                "id": "COUNTER",
                "name": "TEXT(50)",
                "value": "DOUBLE"
            },
            primary_key=["id"]
        )
        
        # Insert test data
        test_data = [
            {"name": "Test 1", "value": 10.5},
            {"name": "Test 2", "value": 20.0},
            {"name": "Test 3", "value": 30.0}
        ]
        
        for record in test_data:
            self.db.insert_record(table_name, record)
            
        # Start transaction
        self.db.begin_transaction()
        
        try:
            # Delete records
            conditions = {"name": "Test 2"}
            self.db.delete_records(table_name, conditions)
            
            # Verify deletion within transaction
            remaining_records = self.db.read_records(table_name)
            assert len(remaining_records) == 2
            
            # Simulate error and rollback
            raise Exception("Simulated error")
            
        except Exception:
            self.db.rollback_transaction()
            
        # Verify rollback
        remaining_records = self.db.read_records(table_name)
        assert len(remaining_records) == 3
        assert any(record["name"] == "Test 2" for record in remaining_records)
        
        # Cleanup
        self.db.drop_table(table_name)

    def test_create_table(self, db_operations):
        """Test creating a table with proper Access syntax."""
        columns = {
            'id': 'INTEGER',
            'name': 'TEXT',
            'value': 'DECIMAL',
            'created_date': 'DATETIME'
        }
        
        try:
            db_operations.create_table('test_create', columns, ['id'])
            
            # Verify table was created
            tables = db_operations.get_tables()
            assert 'test_create' in tables
            
            # Verify column types
            columns = db_operations.get_table_columns('test_create')
            assert 'id' in columns
            assert 'name' in columns
            assert 'value' in columns
            assert 'created_date' in columns
            
        finally:
            db_operations.execute_query('DROP TABLE test_create')
            
    def test_alter_table(self, db_operations):
        """Test altering a table with proper Access syntax."""
        # Create initial table
        columns = {
            'id': 'INTEGER',
            'name': 'TEXT'
        }
        db_operations.create_table('test_alter', columns, ['id'])
        
        try:
            # Add new column
            add_columns = {
                'value': 'DECIMAL',
                'created_date': 'DATETIME'
            }
            db_operations.alter_table('test_alter', add_columns=add_columns)
            
            # Verify new columns
            columns = db_operations.get_table_columns('test_alter')
            assert 'value' in columns
            assert 'created_date' in columns
            
            # Drop column
            db_operations.alter_table('test_alter', drop_columns=['value'])
            
            # Verify column was dropped
            columns = db_operations.get_table_columns('test_alter')
            assert 'value' not in columns
            
        finally:
            db_operations.execute_query('DROP TABLE test_alter')
            
    def test_add_foreign_key(self, db_operations):
        """Test adding a foreign key with proper Access syntax."""
        # Create parent table
        parent_columns = {
            'id': 'INTEGER',
            'name': 'TEXT'
        }
        db_operations.create_table('test_parent', parent_columns, ['id'])
        
        # Create child table
        child_columns = {
            'id': 'INTEGER',
            'parent_id': 'INTEGER',
            'value': 'TEXT'
        }
        db_operations.create_table('test_child', child_columns, ['id'])
        
        try:
            # Add foreign key
            db_operations.add_foreign_key('test_child', 'parent_id', 'test_parent', 'id')
            
            # Verify foreign key was added
            # Note: Access doesn't provide a direct way to query foreign keys
            # We'll verify by attempting to insert invalid data
            db_operations.insert_data('test_parent', {'id': 1, 'name': 'Test'})
            
            # Valid foreign key
            db_operations.insert_data('test_child', {'id': 1, 'parent_id': 1, 'value': 'Test'})
            
            # Invalid foreign key should fail
            with pytest.raises(Exception):
                db_operations.insert_data('test_child', {'id': 2, 'parent_id': 999, 'value': 'Test'})
                
        finally:
            db_operations.execute_query('DROP TABLE test_child')
            db_operations.execute_query('DROP TABLE test_parent')
            
    def test_read_records_with_limit(self, db_operations):
        """Test reading records with LIMIT using Access TOP syntax."""
        # Create test table
        columns = {
            'id': 'INTEGER',
            'name': 'TEXT',
            'value': 'DECIMAL'
        }
        db_operations.create_table('test_limit', columns, ['id'])
        
        try:
            # Insert test data
            for i in range(10):
                db_operations.insert_data('test_limit', {
                    'id': i + 1,
                    'name': f'Test {i + 1}',
                    'value': float(i + 1)
                })
            
            # Test with limit
            results = db_operations.read_records('test_limit', limit=5)
            assert len(results) == 5
            
            # Test with limit and offset (should raise NotImplementedError)
            with pytest.raises(NotImplementedError):
                db_operations.read_records('test_limit', limit=5, offset=5)
                
        finally:
            db_operations.execute_query('DROP TABLE test_limit')
            
    def test_update_record(self, db_operations):
        """Test updating a record with proper Access syntax."""
        # Create test table
        columns = {
            'id': 'INTEGER',
            'name': 'TEXT',
            'value': 'DECIMAL',
            'status': 'TEXT'
        }
        db_operations.create_table('test_update', columns, ['id'])
        
        try:
            # Insert test record
            db_operations.insert_data('test_update', {
                'id': 1,
                'name': 'Test',
                'value': 10.5,
                'status': 'active'
            })
            
            # Update record
            result = db_operations.update_data(
                'test_update',
                {'name': 'Updated', 'value': 20.75, 'status': 'inactive'},
                'id = ?',
                {'id': 1}
            )
            assert result == 1
            
            # Verify update
            results = db_operations.read_records('test_update', {'id': 1})
            assert len(results) == 1
            assert results[0]['name'] == 'Updated'
            assert results[0]['value'] == 20.75
            assert results[0]['status'] == 'inactive'
            
        finally:
            db_operations.execute_query('DROP TABLE test_update')
            
    def test_delete_data(self, db_operations):
        """Test deleting data with proper Access syntax."""
        # Create test table
        columns = {
            'id': 'INTEGER',
            'name': 'TEXT',
            'value': 'DECIMAL'
        }
        db_operations.create_table('test_delete', columns, ['id'])
        
        try:
            # Insert test data
            for i in range(5):
                db_operations.insert_data('test_delete', {
                    'id': i + 1,
                    'name': f'Test {i + 1}',
                    'value': float(i + 1)
                })
            
            # Delete records
            result = db_operations.delete_data(
                'test_delete',
                'value > ?',
                {'value': 3}
            )
            assert result == 2
            
            # Verify deletion
            results = db_operations.read_records('test_delete')
            assert len(results) == 3
            
        finally:
            db_operations.execute_query('DROP TABLE test_delete')

    def test_get_tables(self):
        """Test getting list of tables."""
        # Create test tables
        self.db_operations.execute_query("""
            CREATE TABLE test_table1 (
                id LONG PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        self.db_operations.execute_query("""
            CREATE TABLE test_table2 (
                id LONG PRIMARY KEY,
                value DOUBLE
            )
        """)
        
        # Get tables
        tables = self.db_operations.get_tables()
        
        # Verify results
        self.assertIn('test_table1', tables)
        self.assertIn('test_table2', tables)
        
        # Cleanup
        self.db_operations.drop_table('test_table1')
        self.db_operations.drop_table('test_table2')

    def test_get_table_columns(self):
        """Test getting table columns."""
        # Create test table
        self.db_operations.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY,
                name VARCHAR(255),
                value DOUBLE
            )
        """)
        
        # Get columns
        columns = self.db_operations.get_table_columns('test_table')
        
        # Verify results
        self.assertEqual(['id', 'name', 'value'], columns)
        
        # Cleanup
        self.db_operations.drop_table('test_table')

    def test_table_exists(self):
        """Test table existence check."""
        # Create test table
        self.db_operations.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY
            )
        """)
        
        # Check existence
        self.assertTrue(self.db_operations.table_exists('test_table'))
        self.assertFalse(self.db_operations.table_exists('nonexistent_table'))
        
        # Cleanup
        self.db_operations.drop_table('test_table')

    def test_drop_table(self):
        """Test dropping a table."""
        # Create test table
        self.db_operations.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY
            )
        """)
        
        # Verify table exists
        self.assertTrue(self.db_operations.table_exists('test_table'))
        
        # Drop table
        self.db_operations.drop_table('test_table')
        
        # Verify table no longer exists
        self.assertFalse(self.db_operations.table_exists('test_table'))
        
        # Test dropping non-existent table
        with self.assertRaises(DatabaseError):
            self.db_operations.drop_table('nonexistent_table')

    def test_truncate_table(self):
        """Test truncating a table."""
        # Create and populate test table
        self.db_operations.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY,
                value DOUBLE
            )
        """)
        self.db_operations.execute_query(
            "INSERT INTO test_table (id, value) VALUES (?, ?)",
            (1, 10.0)
        )
        
        # Verify record exists
        count_before = self.db_operations.execute_query(
            "SELECT COUNT(*) as count FROM test_table"
        )[0]['count']
        self.assertEqual(1, count_before)
        
        # Truncate table
        self.db_operations.truncate_table('test_table')
        
        # Verify table is empty
        count_after = self.db_operations.execute_query(
            "SELECT COUNT(*) as count FROM test_table"
        )[0]['count']
        self.assertEqual(0, count_after)
        
        # Cleanup
        self.db_operations.drop_table('test_table')

    def test_get_table_info(self):
        """Test getting table information."""
        # Create test table
        self.db_operations.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY,
                name VARCHAR(255),
                value DOUBLE
            )
        """)
        self.db_operations.execute_query(
            "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
            (1, 'test', 10.0)
        )
        
        # Get table info
        info = self.db_operations.get_table_info('test_table')
        
        # Verify results
        self.assertEqual('test_table', info['name'])
        self.assertEqual(1, info['record_count'])
        self.assertEqual(3, len(info['columns']))
        
        # Verify column information
        column_names = [col['name'] for col in info['columns']]
        self.assertEqual(['id', 'name', 'value'], column_names)
        
        # Cleanup
        self.db_operations.drop_table('test_table')

    def test_verify_record_exists(self):
        """Test record existence verification."""
        # Create and populate test table
        self.db_operations.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        self.db_operations.execute_query(
            "INSERT INTO test_table (id, name) VALUES (?, ?)",
            (1, 'test')
        )
        
        # Test existing record
        self.assertTrue(self.db_operations.verify_record_exists(
            'test_table',
            {'id': 1, 'name': 'test'}
        ))
        
        # Test non-existing record
        self.assertFalse(self.db_operations.verify_record_exists(
            'test_table',
            {'id': 2, 'name': 'nonexistent'}
        ))
        
        # Cleanup
        self.db_operations.drop_table('test_table')

    def test_transaction_management(self):
        """Test transaction management."""
        # Create test table
        self.db_operations.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY,
                value DOUBLE
            )
        """)
        
        # Test successful transaction
        self.db_operations.begin_transaction()
        self.db_operations.execute_query(
            "INSERT INTO test_table (id, value) VALUES (?, ?)",
            (1, 10.0)
        )
        self.db_operations.commit_transaction()
        
        # Verify record was committed
        count = self.db_operations.execute_query(
            "SELECT COUNT(*) as count FROM test_table"
        )[0]['count']
        self.assertEqual(1, count)
        
        # Test transaction rollback
        self.db_operations.begin_transaction()
        self.db_operations.execute_query(
            "INSERT INTO test_table (id, value) VALUES (?, ?)",
            (2, 20.0)
        )
        self.db_operations.rollback_transaction()
        
        # Verify record was not committed
        count = self.db_operations.execute_query(
            "SELECT COUNT(*) as count FROM test_table"
        )[0]['count']
        self.assertEqual(1, count)
        
        # Cleanup
        self.db_operations.drop_table('test_table')

    def test_context_manager(self):
        """Test context manager functionality."""
        with self.db_operations as db:
            # Create test table
            db.execute_query("""
                CREATE TABLE test_table (
                    id LONG PRIMARY KEY
                )
            """)
            
            # Verify table exists
            self.assertTrue(db.table_exists('test_table'))
            
            # Cleanup
            db.drop_table('test_table')
        
        # Verify connection is closed
        self.assertIsNone(self.db_operations.conn)
        self.assertIsNone(self.db_operations.cursor) 