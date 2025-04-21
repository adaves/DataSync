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
from tests.fixtures.database import temp_db_path, db_operations, db_validation, db_monitor
from tests.fixtures.mock_database.create_mock_db import create_mock_database
from datetime import datetime
from datasync.database.sql_syntax import AccessSQLSyntax

@pytest.fixture
def mock_db_path(tmp_path):
    """Create a temporary database path for testing."""
    db_path = tmp_path / "test_database.accdb"
    create_mock_database(str(db_path))
    return db_path

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
    
    def test_connection_management(self, db_operations):
        """Test database connection management."""
        try:
            db_operations.connect()
            assert db_operations.conn is not None
            assert db_operations.cursor is not None
        finally:
            db_operations.close()
    
    def test_table_operations(self, db_operations):
        """Test table creation and management."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestTable (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50),
                Value DOUBLE,
                CreatedDate DATETIME
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Verify table was created
            tables = db_operations.get_tables()
            assert "TestTable" in tables
            
            # Drop the table
            db_operations.execute_query("DROP TABLE TestTable")
            
            # Verify table was dropped
            tables = db_operations.get_tables()
            assert "TestTable" not in tables
            
        finally:
            db_operations.close()
    
    def test_insert_operations(self, db_operations):
        """Test record insertion operations."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestInsert (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50),
                Value DOUBLE,
                CreatedDate DATETIME
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Test single record insertion
            record = {
                "Name": "Test Insert",
                "Value": 42.5,
                "CreatedDate": datetime.now()
            }
            
            result = db_operations.insert_record("TestInsert", record)
            assert result == 1
            
            # Verify the record was inserted
            results = db_operations.execute_query("SELECT * FROM TestInsert")
            assert len(results) == 1
            assert results[0]["Name"] == "Test Insert"
            assert results[0]["Value"] == 42.5
            
            # Test batch insertion
            records = [
                {"Name": f"Test {i}", "Value": i * 10.0, "CreatedDate": datetime.now()}
                for i in range(1, 6)
            ]
            
            result = db_operations.batch_insert("TestInsert", records, batch_size=2)
            assert result == 5
            
            # Verify all records were inserted
            results = db_operations.execute_query("SELECT * FROM TestInsert ORDER BY ID")
            assert len(results) == 6
            for i, record in enumerate(results[1:], 1):
                assert record["Name"] == f"Test {i}"
                assert record["Value"] == i * 10.0
            
        finally:
            # Clean up
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestInsert")
                db_operations.close()
    
    def test_update_operations(self, db_operations):
        """Test record update operations."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestUpdate (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50),
                Value DOUBLE,
                Status TEXT(20)
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Insert test data
            db_operations.execute_query("""
                INSERT INTO TestUpdate (Name, Value, Status)
                VALUES ('Test1', 10.0, 'active'),
                       ('Test2', 20.0, 'inactive'),
                       ('Test3', 30.0, 'active')
            """)
            
            # Test single record update
            record = {
                "ID": 1,
                "Name": "Updated Test1",
                "Value": 15.0
            }
            result = db_operations.update_record("TestUpdate", record, ["ID"])
            assert result == 1
            
            # Verify update
            results = db_operations.execute_query("SELECT * FROM TestUpdate WHERE ID = 1")
            assert len(results) == 1
            assert results[0]["Name"] == "Updated Test1"
            assert results[0]["Value"] == 15.0
            assert results[0]["Status"] == "active"  # Unchanged field
            
            # Test batch update
            records = [
                {"ID": 2, "Status": "active"},
                {"ID": 3, "Status": "inactive"}
            ]
            result = db_operations.batch_update("TestUpdate", records, ["ID"])
            assert result == 2
            
            # Verify updates
            results = db_operations.execute_query("SELECT * FROM TestUpdate ORDER BY ID")
            assert results[1]["Status"] == "active"
            assert results[2]["Status"] == "inactive"
            
        finally:
            # Clean up
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestUpdate")
                db_operations.close()
    
    def test_delete_operations(self, db_operations):
        """Test record deletion operations."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestDelete (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50),
                Status TEXT(20),
                DeletedAt DATETIME
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Insert test data
            db_operations.execute_query("""
                INSERT INTO TestDelete (Name, Status)
                VALUES ('Test1', 'active'),
                       ('Test2', 'inactive'),
                       ('Test3', 'active')
            """)
            
            # Test delete with conditions
            result = db_operations.delete_records("TestDelete", {"Status": "inactive"})
            assert result == 1
            
            # Verify deletion
            results = db_operations.execute_query("SELECT * FROM TestDelete")
            assert len(results) == 2
            assert all(r["Status"] == "active" for r in results)
            
            # Test soft delete
            result = db_operations.soft_delete("TestDelete", 1)
            assert result is True
            
            # Verify soft delete
            results = db_operations.execute_query("SELECT * FROM TestDelete WHERE ID = 1")
            assert len(results) == 1
            assert results[0]["DeletedAt"] is not None
            
        finally:
            # Clean up
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestDelete")
                db_operations.close()
    
    def test_transaction_operations(self, db_operations):
        """Test transaction management operations."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestTransaction (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50) NOT NULL,
                Value DOUBLE
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Test transaction rollback
            db_operations.begin_transaction()
            
            # Insert valid data
            db_operations.execute_query(
                "INSERT INTO TestTransaction (Name, Value) VALUES (?, ?)",
                ("Valid", 42.5)
            )
            
            # Try to insert invalid data (should fail)
            with pytest.raises(pyodbc.Error):
                db_operations.execute_query(
                    "INSERT INTO TestTransaction (Name, Value) VALUES (?, ?)",
                    (None, 99.9)  # This should fail due to NOT NULL constraint
                )
            
            # Rollback the transaction
            db_operations.rollback_transaction()
            
            # Verify no data was inserted
            results = db_operations.execute_query("SELECT * FROM TestTransaction")
            assert len(results) == 0
            
            # Test successful transaction
            db_operations.begin_transaction()
            
            # Insert valid data
            db_operations.execute_query(
                "INSERT INTO TestTransaction (Name, Value) VALUES (?, ?)",
                ("Valid", 42.5)
            )
            
            # Commit the transaction
            db_operations.commit_transaction()
            
            # Verify data was inserted
            results = db_operations.execute_query("SELECT * FROM TestTransaction")
            assert len(results) == 1
            assert results[0]["Name"] == "Valid"
            assert results[0]["Value"] == 42.5
            
        finally:
            # Clean up
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestTransaction")
                db_operations.close()

    def test_initialization(self, db_operations, mock_db_path):
        """Test database operations initialization."""
        assert db_operations.db_path == normalize_path(mock_db_path)
        assert db_operations.conn is None
        assert db_operations.cursor is None
    
    def test_connection_string(self, db_operations):
        """Test connection string generation."""
        conn_str = db_operations.conn_str
        assert "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}" in conn_str
        assert str(db_operations.db_path.absolute()) in conn_str
        # Verify proper path escaping
        assert "\\\\" in conn_str  # Should have double backslashes for ODBC
    
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

    def test_read_records_basic(self, db_operations, mocker):
        """Test basic read_records functionality."""
        # Mock the cursor and its methods
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1"), (2, "Test2")]
        db_operations.cursor = mock_cursor
        
        results = db_operations.read_records("test_table")
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[0]["name"] == "Test1"
        
    def test_read_records_with_filters(self, db_operations, mocker):
        """Test read_records with filters."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1")]
        db_operations.cursor = mock_cursor
        
        filters = {"name": "Test1"}
        results = db_operations.read_records("test_table", filters=filters)
        assert len(results) == 1
        assert results[0]["name"] == "Test1"
        
    def test_read_records_with_sorting(self, db_operations, mocker):
        """Test read_records with sorting."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(2, "Test2"), (1, "Test1")]
        db_operations.cursor = mock_cursor
        
        results = db_operations.read_records("test_table", sort_by=["id"], sort_desc=True)
        assert results[0]["id"] == 2
        assert results[1]["id"] == 1
        
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

    def test_delete_records(self, db_operations, temp_db_path):
        """Test basic delete operation with conditions."""
        # Create test table
        db_operations.execute_query("""
            CREATE TABLE test_delete (
                id INTEGER PRIMARY KEY,
                name TEXT,
                status TEXT
            )
        """)
        
        # Insert test data
        db_operations.execute_query("""
            INSERT INTO test_delete (id, name, status)
            VALUES (1, 'Test1', 'active'),
                   (2, 'Test2', 'inactive'),
                   (3, 'Test3', 'active')
        """)
        
        # Delete records with condition
        deleted_count = db_operations.delete_records("test_delete", {"status": "inactive"})
        assert deleted_count == 1
        
        # Verify remaining records
        result = db_operations.execute_query("SELECT * FROM test_delete")
        assert len(result) == 2
        assert all(record["status"] == "active" for record in result)

    def test_soft_delete(self, db_operations, temp_db_path):
        """Test soft delete functionality."""
        # Create test table with deleted_at column
        db_operations.execute_query("""
            CREATE TABLE test_soft_delete (
                id INTEGER PRIMARY KEY,
                name TEXT,
                deleted_at DATETIME
            )
        """)
        
        # Insert test data
        db_operations.execute_query("""
            INSERT INTO test_soft_delete (id, name)
            VALUES (1, 'Test1'),
                   (2, 'Test2')
        """)
        
        # Perform soft delete
        success = db_operations.soft_delete("test_soft_delete", 1)
        assert success is True
        
        # Verify record is marked as deleted
        result = db_operations.execute_query("SELECT * FROM test_soft_delete WHERE id = 1")
        assert len(result) == 1
        assert result[0]["deleted_at"] is not None

    def test_cascade_delete(self, db_operations, temp_db_path):
        """Test cascade delete functionality."""
        # Create parent and child tables
        db_operations.execute_query("""
            CREATE TABLE test_parent (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        
        db_operations.execute_query("""
            CREATE TABLE test_child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                name TEXT,
                FOREIGN KEY (parent_id) REFERENCES test_parent(id)
            )
        """)
        
        # Insert test data
        db_operations.execute_query("""
            INSERT INTO test_parent (id, name)
            VALUES (1, 'Parent1')
        """)
        
        db_operations.execute_query("""
            INSERT INTO test_child (id, parent_id, name)
            VALUES (1, 1, 'Child1'),
                   (2, 1, 'Child2')
        """)
        
        # Configure cascade delete
        cascade_config = [
            {"table": "test_child", "foreign_key": "parent_id", "cascade_type": "delete"}
        ]
        
        # Perform cascade delete
        results = db_operations.cascade_delete("test_parent", 1, cascade_config)
        assert results["test_parent"] == 1
        assert results["test_child"] == 2
        
        # Verify records are deleted
        parent_count = db_operations.execute_query("SELECT COUNT(*) as count FROM test_parent")[0]["count"]
        child_count = db_operations.execute_query("SELECT COUNT(*) as count FROM test_child")[0]["count"]
        assert parent_count == 0
        assert child_count == 0

    def test_delete_with_transaction(self, db_operations, temp_db_path):
        """Test delete operation within a transaction."""
        # Create test table
        db_operations.execute_query("""
            CREATE TABLE test_transaction (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        
        # Insert test data
        db_operations.execute_query("""
            INSERT INTO test_transaction (id, name)
            VALUES (1, 'Test1'),
                   (2, 'Test2')
        """)
        
        # Perform delete within transaction
        deleted_count = db_operations.delete_with_transaction("test_transaction", {"id": 1})
        assert deleted_count == 1
        
        # Verify record is deleted
        result = db_operations.execute_query("SELECT * FROM test_transaction")
        assert len(result) == 1
        assert result[0]["id"] == 2

    def test_delete_transaction_rollback(self, db_operations, temp_db_path):
        """Test transaction rollback during delete operation."""
        # Create test table
        db_operations.execute_query("""
            CREATE TABLE test_rollback (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        
        # Insert test data
        db_operations.execute_query("""
            INSERT INTO test_rollback (id, name)
            VALUES (1, 'Test1')
        """)
        
        # Attempt delete with invalid condition to force rollback
        try:
            db_operations.delete_with_transaction("test_rollback", {"invalid_column": "value"})
        except Exception:
            pass
        
        # Verify record still exists after rollback
        result = db_operations.execute_query("SELECT * FROM test_rollback")
        assert len(result) == 1
        assert result[0]["id"] == 1 

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