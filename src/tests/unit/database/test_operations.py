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

# Test database connection string
TEST_DB_CONNECTION_STRING = "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=tests/fixtures/mock_database/test.accdb"

@pytest.fixture
def mock_db_path(tmp_path):
    """Create a temporary database file for testing."""
    # Ensure the directory exists
    tmp_path.mkdir(parents=True, exist_ok=True)
    
    # Create the database file
    db_path = tmp_path / "test_database.accdb"
    create_mock_database(str(db_path))
    
    return str(db_path)

@pytest.fixture
def db_ops(tmp_path):
    """Create a DatabaseOperations instance for testing."""
    # Create a temporary database file path
    db_path = tmp_path / "test.accdb"
    
    # Create the database operations instance
    ops = DatabaseOperations(str(db_path))
    ops.connect()  # This will create the database if it doesn't exist
    
    yield ops
    
    try:
        ops.disconnect()
    except Exception:
        pass  # Ignore cleanup errors

@pytest.fixture
def setup_test_table(db_ops):
    """Create and clean up test table."""
    table_name = "test_table"
    
    # Clean up if table exists
    try:
        db_ops.execute_query(f"DROP TABLE [{table_name}]")
    except Exception:
        pass  # Ignore if table doesn't exist
        
    # Create test table
    columns = {
        "id": "INT IDENTITY(1,1)",
        "name": "NVARCHAR(50)",
        "value": "INT"
    }
    db_ops.create_table(table_name, columns, primary_key=["id"])
    
    # Insert some test data
    test_data = [
        {"name": "test1", "value": 1},
        {"name": "test2", "value": 2},
        {"name": "test3", "value": 3}
    ]
    for data in test_data:
        db_ops.insert_record(table_name, data)
        
    yield table_name
    
    # Clean up
    try:
        db_ops.execute_query(f"DROP TABLE [{table_name}]")
    except Exception:
        pass  # Ignore cleanup errors

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
    def setup_method(self, db_ops):
        """Set up test environment before each test method."""
        self.db = db_ops
        self.test_table_name = "test_table"
        
        # Create test table using DatabaseOperations methods
        try:
            self.db.execute_query("""
                CREATE TABLE [test_table] (
                    [id] COUNTER PRIMARY KEY,
                    [name] TEXT(50),
                    [value] DOUBLE,
                    [category] TEXT(10),
                    [date_field] DATETIME
                )
            """)
            
            # Insert some test data
            test_data = [
                {"name": "Test 1", "value": 10.5, "category": "A", "date_field": datetime(2023, 1, 1)},
                {"name": "Test 2", "value": 20.0, "category": "A", "date_field": datetime(2023, 6, 1)},
                {"name": "Test 3", "value": 30.0, "category": "B", "date_field": datetime(2024, 1, 1)}
            ]
            
            for record in test_data:
                self.db.insert_record(self.test_table_name, record)
                
            self.db.commit()
            
        except Exception as e:
            if "already exists" not in str(e):
                if hasattr(self.db, 'in_transaction') and self.db.in_transaction:
                    self.db.rollback()
                raise
            self.db.rollback()

    def teardown_method(self):
        """Clean up after each test method."""
        if hasattr(self, 'db') and self.db is not None:
            try:
                # Drop test table if it exists
                self.db.execute_query(f"DROP TABLE [{self.test_table_name}]")
                self.db.commit()
            except Exception:
                pass  # Ignore cleanup errors
            
            if hasattr(self.db, 'in_transaction') and self.db.in_transaction:
                self.db.rollback()

    def test_connection_management(self, db_ops):
        """Test database connection management."""
        assert db_ops.conn is not None
        assert db_ops.cursor is not None
        
        db_ops.close()
        assert db_ops.conn is None
        assert db_ops.cursor is None
        
        db_ops.connect()
        assert db_ops.conn is not None
        assert db_ops.cursor is not None

    def test_table_operations(self, db_ops):
        """Test basic table operations."""
        # Create test table
        db_ops.create_table(
            "test_table",
            {
                "id": "LONG",
                "name": "VARCHAR(255)",
                "value": "DOUBLE"
            },
            primary_key=["id"]
        )
        
        # Verify table exists
        tables = db_ops.get_tables()
        assert "test_table" in tables
        
        # Get columns
        columns = db_ops.get_table_columns("test_table")
        assert "id" in columns
        assert "name" in columns
        assert "value" in columns
        
        # Clean up
        db_ops.execute_query("DROP TABLE [test_table]")

    def test_insert_operations(self, db_ops, setup_test_table):
        """Test insert operations with comprehensive validation and transaction handling."""
        table_name = setup_test_table
        
        # Test basic insert
        record = {
            "id": 4,
            "name": "Test 4",
            "value": 40.0,
            "category": "B",
            "date_field": datetime(2024, 6, 1)
        }
        result = db_ops.insert_record(table_name, record)
        assert result == 1
        
        # Verify insert
        query = f"SELECT * FROM [{table_name}] WHERE [id] = 4"
        result = db_ops.execute_query(query)
        assert len(result) == 1
        assert result[0]["name"] == "Test 4"
        
        # Test insert with missing required field
        with pytest.raises(ValueError) as exc_info:
            db_ops.insert_record(table_name, {
                "name": "Test 5",
                "value": 50.0
            })
        assert "required field" in str(exc_info.value).lower()
            
        # Test insert with invalid data type
        with pytest.raises(ValueError) as exc_info:
            db_ops.insert_record(table_name, {
                "id": 5,
                "name": "Test 5",
                "value": "invalid",  # Should be a number
                "category": "B",
                "date_field": datetime(2024, 6, 1)
            })
        assert "invalid data type" in str(exc_info.value).lower()
            
        # Test insert with duplicate primary key
        with pytest.raises(ValueError) as exc_info:
            db_ops.insert_record(table_name, {
                "id": 4,  # Already exists
                "name": "Test 6",
                "value": 60.0,
                "category": "B",
                "date_field": datetime(2024, 6, 1)
            })
        assert "duplicate" in str(exc_info.value).lower()
            
        # Test insert with transaction and commit
        db_ops.begin_transaction()
        try:
            record = {
                "id": 6,
                "name": "Test 6",
                "value": 60.0,
                "category": "B",
                "date_field": datetime(2024, 6, 1)
            }
            result = db_ops.insert_record(table_name, record)
            assert result == 1
            
            # Verify insert before commit
            query = f"SELECT * FROM [{table_name}] WHERE [id] = 6"
            result = db_ops.execute_query(query)
            assert len(result) == 1
            
            db_ops.commit()
            
            # Verify insert after commit
            result = db_ops.execute_query(query)
            assert len(result) == 1
            assert result[0]["name"] == "Test 6"
        except Exception:
            db_ops.rollback()
            raise
            
        # Test insert with transaction and rollback
        db_ops.begin_transaction()
        try:
            record = {
                "id": 7,
                "name": "Test 7",
                "value": 70.0,
                "category": "B",
                "date_field": datetime(2024, 6, 1)
            }
            result = db_ops.insert_record(table_name, record)
            assert result == 1
            
            # Verify insert before rollback
            query = f"SELECT * FROM [{table_name}] WHERE [id] = 7"
            result = db_ops.execute_query(query)
            assert len(result) == 1
            
            db_ops.rollback()
            
            # Verify rollback
            result = db_ops.execute_query(query)
            assert len(result) == 0
        except Exception:
            db_ops.rollback()
            raise
            
        # Test insert with null values
        record = {
            "id": 8,
            "name": "Test 8",
            "value": None,  # Null value
            "category": "B",
            "date_field": datetime(2024, 6, 1)
        }
        result = db_ops.insert_record(table_name, record)
        assert result == 1
        
        # Verify null value insert
        query = f"SELECT * FROM [{table_name}] WHERE [id] = 8"
        result = db_ops.execute_query(query)
        assert len(result) == 1
        assert result[0]["value"] is None
        
        # Test insert with maximum field lengths
        record = {
            "id": 9,
            "name": "A" * 50,  # Maximum length for name field
            "value": 90.0,
            "category": "B" * 10,  # Maximum length for category field
            "date_field": datetime(2024, 6, 1)
        }
        result = db_ops.insert_record(table_name, record)
        assert result == 1
        
        # Verify maximum length insert
        query = f"SELECT * FROM [{table_name}] WHERE [id] = 9"
        result = db_ops.execute_query(query)
        assert len(result) == 1
        assert len(result[0]["name"]) == 50
        assert len(result[0]["category"]) == 10
        
        # Test insert with invalid date format
        with pytest.raises(ValueError) as exc_info:
            db_ops.insert_record(table_name, {
                "id": 10,
                "name": "Test 10",
                "value": 100.0,
                "category": "B",
                "date_field": "invalid_date"  # Invalid date format
            })
        assert "invalid date" in str(exc_info.value).lower()
        
        # Test insert with future date validation
        future_date = datetime.now().replace(year=datetime.now().year + 1)
        with pytest.raises(ValueError) as exc_info:
            db_ops.insert_record(table_name, {
                "id": 11,
                "name": "Test 11",
                "value": 110.0,
                "category": "B",
                "date_field": future_date  # Future date
            })
        assert "future date" in str(exc_info.value).lower()

    def test_update_operations(self, db_ops, setup_test_table):
        """Test update operations with comprehensive validation and transaction handling."""
        table_name = setup_test_table
        
        # Test basic update with column list conditions
        update_data = {
            "id": 1,
            "name": "Updated Test 1",
            "value": 15.5
        }
        result = db_ops.update_record(table_name, update_data, ["id"])
        assert result == 1
        
        # Verify update
        query = f"SELECT * FROM [{table_name}] WHERE [id] = 1"
        result = db_ops.execute_query(query)
        assert len(result) == 1
        assert result[0]["name"] == "Updated Test 1"
        assert result[0]["value"] == 15.5
        
        # Test update with invalid data type
        with pytest.raises(ValueError) as exc_info:
            db_ops.update_record(table_name, {
                "id": 2,
                "value": "invalid"  # Should be a number
            }, ["id"])
        assert "invalid data type" in str(exc_info.value).lower()
            
        # Test update with non-existent record
        result = db_ops.update_record(table_name, {
            "id": 999,
            "name": "Non-existent"
        }, ["id"])
        assert result == 0  # No records updated
        
        # Test update with transaction and commit
        db_ops.begin_transaction()
        try:
            update_data = {
                "id": 2,
                "name": "Updated Test 2",
                "value": 25.5
            }
            result = db_ops.update_record(table_name, update_data, ["id"])
            assert result == 1
            
            # Verify update before commit
            query = f"SELECT * FROM [{table_name}] WHERE [id] = 2"
            result = db_ops.execute_query(query)
            assert len(result) == 1
            assert result[0]["name"] == "Updated Test 2"
            assert result[0]["value"] == 25.5
            
            db_ops.commit()
            
            # Verify update after commit
            result = db_ops.execute_query(query)
            assert len(result) == 1
            assert result[0]["name"] == "Updated Test 2"
            assert result[0]["value"] == 25.5
        except Exception:
            db_ops.rollback()
            raise
            
        # Test update with transaction and rollback
        db_ops.begin_transaction()
        try:
            update_data = {
                "id": 3,
                "name": "Updated Test 3",
                "value": 35.5
            }
            result = db_ops.update_record(table_name, update_data, ["id"])
            assert result == 1
            
            # Verify update before rollback
            query = f"SELECT * FROM [{table_name}] WHERE [id] = 3"
            result = db_ops.execute_query(query)
            assert len(result) == 1
            assert result[0]["name"] == "Updated Test 3"
            assert result[0]["value"] == 35.5
            
            db_ops.rollback()
            
            # Verify rollback
            result = db_ops.execute_query(query)
            assert result[0]["name"] == "Test 3"  # Original value
            assert result[0]["value"] == 30.0  # Original value
        except Exception:
            db_ops.rollback()
            raise
            
        # Test update with dictionary conditions
        update_data = {
            "category": "C",
            "value": 100.0
        }
        result = db_ops.update_record(table_name, update_data, {
            "category": "A",
            "value": 15.5
        })
        assert result == 1
        
        # Verify multiple condition update
        query = f"SELECT * FROM [{table_name}] WHERE [category] = 'C'"
        result = db_ops.execute_query(query)
        assert len(result) == 1
        assert result[0]["value"] == 100.0
        
        # Test update with null values
        update_data = {
            "id": 1,
            "name": None,  # Null value
            "value": 50.0
        }
        result = db_ops.update_record(table_name, update_data, ["id"])
        assert result == 1
        
        # Verify null value update
        query = f"SELECT * FROM [{table_name}] WHERE [id] = 1"
        result = db_ops.execute_query(query)
        assert len(result) == 1
        assert result[0]["name"] is None
        assert result[0]["value"] == 50.0
        
        # Test update with maximum field lengths
        update_data = {
            "id": 2,
            "name": "A" * 50,  # Maximum length for name field
            "category": "B" * 10  # Maximum length for category field
        }
        result = db_ops.update_record(table_name, update_data, ["id"])
        assert result == 1
        
        # Verify maximum length update
        query = f"SELECT * FROM [{table_name}] WHERE [id] = 2"
        result = db_ops.execute_query(query)
        assert len(result) == 1
        assert len(result[0]["name"]) == 50
        assert len(result[0]["category"]) == 10
        
        # Test update with invalid date format
        with pytest.raises(ValueError) as exc_info:
            db_ops.update_record(table_name, {
                "id": 3,
                "date_field": "invalid_date"  # Invalid date format
            }, ["id"])
        assert "invalid date" in str(exc_info.value).lower()
        
        # Test update with future date validation
        future_date = datetime.now().replace(year=datetime.now().year + 1)
        with pytest.raises(ValueError) as exc_info:
            db_ops.update_record(table_name, {
                "id": 3,
                "date_field": future_date  # Future date
            }, ["id"])
        assert "future date" in str(exc_info.value).lower()
        
        # Test batch update
        update_records = [
            {"id": 1, "name": "Batch Update 1", "value": 100.0},
            {"id": 2, "name": "Batch Update 2", "value": 200.0},
            {"id": 3, "name": "Batch Update 3", "value": 300.0}
        ]
        result = db_ops.batch_update(table_name, update_records, ["id"])
        assert result == 3
        
        # Verify batch update
        for i in range(1, 4):
            query = f"SELECT * FROM [{table_name}] WHERE [id] = {i}"
            result = db_ops.execute_query(query)
            assert len(result) == 1
            assert result[0]["name"] == f"Batch Update {i}"
            assert result[0]["value"] == float(i) * 100.0
        
        # Test update with complex conditions
        update_data = {
            "category": "D",
            "value": 500.0
        }
        conditions = {
            "category": "C",
            "value": {"min": 50.0, "max": 150.0}
        }
        result = db_ops.update_record(table_name, update_data, conditions)
        assert result == 1
        
        # Verify complex condition update
        query = f"SELECT * FROM [{table_name}] WHERE [category] = 'D'"
        result = db_ops.execute_query(query)
        assert len(result) == 1
        assert result[0]["value"] == 500.0

    def test_delete_operations(self, db_ops, setup_test_table):
        """Test delete operations."""
        table_name = setup_test_table
        
        # Delete record
        result = db_ops.delete_records(table_name, {"id": 1})
        assert result == 1
        
        # Verify delete
        query = f"SELECT * FROM [{table_name}] WHERE [id] = 1"
        result = db_ops.execute_query(query)
        assert len(result) == 0
        
        # Test delete with non-existent record
        result = db_ops.delete_records(table_name, {"id": 999})
        assert result == 0  # No records deleted
        
        # Test delete with transaction
        db_ops.begin_transaction()
        try:
            result = db_ops.delete_records(table_name, {"id": 2})
            assert result == 1
            db_ops.commit()
            
            # Verify delete
            query = f"SELECT * FROM [{table_name}] WHERE [id] = 2"
            result = db_ops.execute_query(query)
            assert len(result) == 0
        except Exception:
            db_ops.rollback()
            raise
            
        # Test delete with rollback
        db_ops.begin_transaction()
        try:
            result = db_ops.delete_records(table_name, {"id": 3})
            assert result == 1
            db_ops.rollback()
            
            # Verify rollback
            query = f"SELECT * FROM [{table_name}] WHERE [id] = 3"
            result = db_ops.execute_query(query)
            assert len(result) == 1  # Record still exists
        except Exception:
            db_ops.rollback()
            raise
            
        # Test delete with multiple conditions
        result = db_ops.delete_records(table_name, {
            "category": "B",
            "value": 30.0
        })
        assert result == 1
        
        # Verify multiple condition delete
        query = f"SELECT * FROM [{table_name}] WHERE [category] = 'B' AND [value] = 30.0"
        result = db_ops.execute_query(query)
        assert len(result) == 0
        
        # Test delete all records
        result = db_ops.delete_records(table_name, {})
        assert result > 0
        
        # Verify all records deleted
        query = f"SELECT COUNT(*) as count FROM [{table_name}]"
        result = db_ops.execute_query(query)
        assert result[0]["count"] == 0

    def test_transaction_operations(self, db_ops):
        """Test transaction operations including commit and rollback."""
        # Create test table
        db_ops.execute_query("""
            CREATE TABLE test_transactions (
                id INTEGER PRIMARY KEY,
                value TEXT(50)
            )
        """)
        
        try:
            # Test 1: Transaction commit
            db_ops.begin_transaction()
            db_ops.execute_query(
                "INSERT INTO test_transactions (id, value) VALUES (?, ?)",
                (1, "Test 1")
            )
            db_ops.commit()
            
            # Verify record was committed
            result = db_ops.execute_query("SELECT * FROM test_transactions WHERE id = 1")
            assert len(result) == 1
            assert result[0]["value"] == "Test 1"
            
            # Test 2: Transaction rollback
            db_ops.begin_transaction()
            db_ops.execute_query(
                "INSERT INTO test_transactions (id, value) VALUES (?, ?)",
                (2, "Test 2")
            )
            
            # Verify record is visible within transaction
            result = db_ops.execute_query("SELECT * FROM test_transactions WHERE id = 2")
            assert len(result) == 1
            assert result[0]["value"] == "Test 2"
            
            # Rollback transaction
            db_ops.rollback()
            
            # Verify record was not committed
            result = db_ops.execute_query("SELECT * FROM test_transactions WHERE id = 2")
            assert len(result) == 0
            
        finally:
            # Cleanup
            try:
                db_ops.execute_query("DROP TABLE test_transactions")
            except Exception:
                pass  # Ignore cleanup errors
            
            if db_ops.in_transaction:
                db_ops.rollback()

    def test_initialization(self, db_ops, mock_db_path):
        """Test database operations initialization."""
        assert db_ops.db_path == normalize_path(mock_db_path)
        assert db_ops.conn is None
        assert db_ops.cursor is None
    
    def test_connection_string(self, db_ops, tmp_path):
        """Test connection string generation."""
        # Test with absolute path
        test_db = tmp_path / "database.accdb"
        db_ops.db_path = test_db
        conn_str = db_ops.conn_str
        
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
        db_ops.db_path = relative_db
        conn_str = db_ops.conn_str
        assert "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}" in conn_str
        assert "DBQ=" in conn_str
        expected_path = str(relative_db.absolute()).replace("\\", "\\\\")
        assert expected_path == conn_str.split("DBQ=")[1]
    
    def test_get_tables(self, db_ops, mocker):
        """Test getting list of tables."""
        # Mock the cursor and its tables method
        mock_cursor = mocker.MagicMock()
        mock_cursor.tables.return_value = [
            mocker.MagicMock(table_name="Table1", table_type="TABLE"),
            mocker.MagicMock(table_name="Table2", table_type="VIEW"),
            mocker.MagicMock(table_name="Table3", table_type="TABLE")
        ]
        db_ops.cursor = mock_cursor
        
        tables = db_ops.get_tables()
        assert len(tables) == 2
        assert "Table1" in tables
        assert "Table3" in tables
    
    def test_execute_query_select(self, db_ops, mocker):
        """Test executing a SELECT query."""
        # Create test table
        columns = {
            'id': 'COUNTER',
            'name': 'TEXT(50)'
        }
        db_ops.create_table('test_table', columns, ['id'])
        
        # Insert test data
        db_ops.execute_query(
            "INSERT INTO test_table (name) VALUES (?)",
            ("Test1",)
        )
        
        # Mock the cursor and its methods
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1"), (2, "Test2")]
        db_ops.cursor = mock_cursor
        
        results = db_ops.execute_query("SELECT * FROM test_table")
        assert len(results) == 2
        assert results[0]['id'] == 1
        assert results[0]['name'] == "Test1"
        
        # Cleanup
        db_ops.execute_query("DROP TABLE test_table")
    
    def test_execute_query_insert(self, db_ops, mocker):
        """Test executing an INSERT query."""
        # Mock the cursor and its methods
        mock_cursor = mocker.MagicMock()
        mock_cursor.rowcount = 1
        db_ops.cursor = mock_cursor
        db_ops.conn = mocker.MagicMock()
        
        affected_rows = db_ops.execute_query(
            "INSERT INTO test_table (name) VALUES (?)",
            ("Test",)
        )
        assert affected_rows == 1
    
    def test_get_table_columns(self, db_ops, mocker):
        """Test getting table columns."""
        # Mock the cursor and its description
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",), ("value",)]
        db_ops.cursor = mock_cursor
        
        columns = db_ops.get_table_columns("test_table")
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
        self.db.commit()
        
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
        self.db.commit()
        
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
        self.db.commit()
        
        # Test reading with filters
        filters = {
            'category': 'A',
            'value': {'min': 15.0, 'max': 25.0}
        }
        records = self.db.read_records(self.test_table_name, filters=filters)
        
        assert len(records) == 1
        assert records[0]['value'] == 20.0
        assert records[0]['category'] == 'A'

    def test_read_records_with_sorting(self, db_ops, mocker):
        """Test read_records with sorting."""
        # Create test table
        columns = {
            'id': 'COUNTER',
            'name': 'TEXT(50)',
            'value': 'DOUBLE',
            'date_field': 'DATETIME'
        }
        db_ops.create_table('test_table', columns, ['id'])
        
        try:
            # Insert test data
            test_data = [
                {'name': 'Test1', 'value': 10.0, 'date_field': '2024-01-01'},
                {'name': 'Test2', 'value': 20.0, 'date_field': '2024-01-02'},
                {'name': 'Test3', 'value': 30.0, 'date_field': '2024-01-03'}
            ]
            
            for record in test_data:
                db_ops.insert_record('test_table', record)
            
            # Mock the cursor and its methods
            mock_cursor = mocker.MagicMock()
            mock_cursor.description = [("id",), ("name",), ("value",), ("date_field",)]
            mock_cursor.fetchall.return_value = [
                (3, "Test3", 30.0, "2024-01-03"),
                (2, "Test2", 20.0, "2024-01-02"),
                (1, "Test1", 10.0, "2024-01-01")
            ]
            db_ops.cursor = mock_cursor
            
            # Test descending sort
            results = db_ops.read_records("test_table", sort_by="value", sort_order="DESC")
            
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
            results = db_ops.read_records("test_table", sort_by="value", sort_order="ASC")
            
            assert len(results) == 3
            assert results[0]['value'] == 10.0
            assert results[1]['value'] == 20.0
            assert results[2]['value'] == 30.0
            
        finally:
            # Clean up
            if db_ops.conn:
                db_ops.execute_query('DROP TABLE test_table')
                db_ops.close()

    def test_read_records_with_pagination(self, db_ops, mocker):
        """Test read_records with pagination."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(3, "Test3"), (4, "Test4")]
        db_ops.cursor = mock_cursor
        
        results = db_ops.read_records("test_table", limit=2, offset=2)
        assert len(results) == 2
        assert results[0]["id"] == 3
        
    def test_build_query_basic(self, db_ops):
        """Test basic query building."""
        query = db_ops.build_query("test_table")
        assert query == "SELECT * FROM [test_table]"
        
    def test_build_query_with_columns(self, db_ops):
        """Test query building with specific columns."""
        query = db_ops.build_query("test_table", columns=["id", "name"])
        assert query == "SELECT [id], [name] FROM [test_table]"
        
    def test_build_query_with_joins(self, db_ops):
        """Test query building with joins."""
        joins = [{"table": "other_table", "on": "test_table.id = other_table.test_id"}]
        query = db_ops.build_query("test_table", joins=joins)
        assert "INNER JOIN [other_table] ON test_table.id = other_table.test_id" in query
        
    def test_build_query_with_filters(self, db_ops):
        """Test query building with filters."""
        filters = {"name": "Test1", "status": "active"}
        query = db_ops.build_query("test_table", filters=filters)
        assert "[name] = ?" in query
        assert "[status] = ?" in query
        assert "WHERE" in query
        
    def test_build_query_with_group_by(self, db_ops):
        """Test query building with GROUP BY."""
        query = db_ops.build_query("test_table", group_by=["status"])
        assert "GROUP BY [status]" in query
        
    def test_build_query_with_having(self, db_ops):
        """Test query building with HAVING clause."""
        having = {"count": 5}
        query = db_ops.build_query("test_table", having=having)
        assert "HAVING [count] = ?" in query
        
    def test_build_query_with_sorting(self, db_ops):
        """Test query building with sorting."""
        query = db_ops.build_query("test_table", sort_by=["name"], sort_desc=True)
        assert "ORDER BY [name] DESC" in query
        
    def test_build_query_with_pagination(self, db_ops):
        """Test query building with pagination."""
        query = db_ops.build_query("test_table", limit=10, offset=20)
        assert "LIMIT 10 OFFSET 20" in query

    def test_read_records_with_in_clause(self, db_ops, mocker):
        """Test read_records with IN clause."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1"), (2, "Test2")]
        db_ops.cursor = mock_cursor
        
        filters = {"id": [1, 2, 3]}
        results = db_ops.read_records("test_table", filters=filters)
        assert len(results) == 2
        assert results[0]["id"] in [1, 2]
        
    def test_read_records_with_date_range(self, db_ops, mocker):
        """Test read_records with date range filtering."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("date",)]
        mock_cursor.fetchall.return_value = [(1, "2023-01-01"), (2, "2023-01-15")]
        db_ops.cursor = mock_cursor
        
        date_range = {
            "column": "date",
            "start_date": "2023-01-01",
            "end_date": "2023-01-31"
        }
        results = db_ops.read_records("test_table", date_range=date_range)
        assert len(results) == 2
        assert results[0]["date"] == "2023-01-01"
        
    def test_read_records_with_custom_filters(self, db_ops, mocker):
        """Test read_records with custom SQL filters."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1")]
        db_ops.cursor = mock_cursor
        
        custom_filters = ["LENGTH([name]) > 4", "[id] % 2 = 1"]
        results = db_ops.read_records("test_table", custom_filters=custom_filters)
        assert len(results) == 1
        assert results[0]["id"] == 1
        
    def test_aggregate_query_basic(self, db_ops, mocker):
        """Test basic aggregate query."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("total_amount",), ("count",)]
        mock_cursor.fetchall.return_value = [(1000, 5)]
        db_ops.cursor = mock_cursor
        
        aggregates = {
            "total_amount": "SUM(amount)",
            "count": "COUNT(*)"
        }
        results = db_ops.aggregate_query("test_table", aggregates)
        assert len(results) == 1
        assert results[0]["total_amount"] == 1000
        assert results[0]["count"] == 5
        
    def test_aggregate_query_with_grouping(self, db_ops, mocker):
        """Test aggregate query with grouping."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("category",), ("total_amount",)]
        mock_cursor.fetchall.return_value = [("A", 500), ("B", 300)]
        db_ops.cursor = mock_cursor
        
        aggregates = {"total_amount": "SUM(amount)"}
        group_by = ["category"]
        results = db_ops.aggregate_query("test_table", aggregates, group_by)
        assert len(results) == 2
        assert results[0]["category"] == "A"
        assert results[0]["total_amount"] == 500
        
    def test_aggregate_query_with_having(self, db_ops, mocker):
        """Test aggregate query with HAVING clause."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("category",), ("total_amount",)]
        mock_cursor.fetchall.return_value = [("A", 1000)]
        db_ops.cursor = mock_cursor
        
        aggregates = {"total_amount": "SUM(amount)"}
        group_by = ["category"]
        having = {"total_amount": 1000}
        results = db_ops.aggregate_query("test_table", aggregates, group_by, having=having)
        assert len(results) == 1
        assert results[0]["total_amount"] == 1000
        
    def test_subquery_basic(self, db_ops, mocker):
        """Test basic subquery functionality."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1")]
        db_ops.cursor = mock_cursor
        
        subquery = "SELECT id FROM other_table WHERE status = ?"
        subquery_params = ("active",)
        results = db_ops.subquery("test_table", subquery, subquery_params)
        assert len(results) == 1
        assert results[0]["id"] == 1
        
    def test_subquery_with_filters(self, db_ops, mocker):
        """Test subquery with additional filters."""
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test1")]
        db_ops.cursor = mock_cursor
        
        subquery = "SELECT id FROM other_table WHERE status = ?"
        subquery_params = ("active",)
        filters = {"name": "Test1"}
        results = db_ops.subquery("test_table", subquery, subquery_params, filters)
        assert len(results) == 1
        assert results[0]["name"] == "Test1"

    def test_update_record(self, db_ops):
        """Test updating a single record."""
        # Create test table
        db_ops.execute_query("""
            CREATE TABLE test_update (
                id INTEGER PRIMARY KEY,
                name TEXT(50),
                value DECIMAL(10,2),
                status TEXT(20)
            )
        """)
        
        try:
            # Insert test record
            db_ops.execute_query(
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
            result = db_ops.update_record("test_update", record, ["id"])
            assert result == 1
            
            # Verify update
            results = db_ops.execute_query("SELECT * FROM test_update WHERE id = 1")
            assert len(results) == 1
            assert results[0]["name"] == "Updated"
            assert results[0]["value"] == 20.75
            assert results[0]["status"] == "inactive"
            
        finally:
            db_ops.execute_query("DROP TABLE test_update")

    def test_update_record_nonexistent(self, db_ops):
        """Test updating a non-existent record."""
        # Create test table
        db_ops.execute_query("""
            CREATE TABLE test_update_nonexistent (
                id INTEGER PRIMARY KEY,
                name TEXT(50)
            )
        """)
        
        try:
            # Try to update non-existent record
            record = {"id": 1, "name": "Test"}
            result = db_ops.update_record("test_update_nonexistent", record, ["id"])
            assert result == 0
            
        finally:
            db_ops.execute_query("DROP TABLE test_update_nonexistent")

    def test_batch_update(self, db_ops):
        """Test batch updating multiple records."""
        # Create test table
        db_ops.execute_query("""
            CREATE TABLE test_batch_update (
                id INTEGER PRIMARY KEY,
                name TEXT(50),
                value DECIMAL(10,2)
            )
        """)
        
        try:
            # Insert test records
            for i in range(1, 6):
                db_ops.execute_query(
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
            result = db_ops.batch_update("test_batch_update", records, ["id"], batch_size=2)
            assert result == 3
            
            # Verify updates
            results = db_ops.execute_query("SELECT * FROM test_batch_update ORDER BY id")
            assert len(results) == 5
            assert results[0]["name"] == "Updated 1"
            assert results[0]["value"] == 100.0
            assert results[1]["name"] == "Updated 2"
            assert results[1]["value"] == 200.0
            assert results[2]["name"] == "Updated 3"
            assert results[2]["value"] == 300.0
            
        finally:
            db_ops.execute_query("DROP TABLE test_batch_update")

    def test_batch_update_empty(self, db_ops):
        """Test batch update with empty records list."""
        # Create test table
        db_ops.execute_query("""
            CREATE TABLE test_batch_update_empty (
                id INTEGER PRIMARY KEY,
                name TEXT(50)
            )
        """)
        
        try:
            # Try batch update with empty list
            result = db_ops.batch_update("test_batch_update_empty", [], ["id"])
            assert result == 0
            
        finally:
            db_ops.execute_query("DROP TABLE test_batch_update_empty")

    def test_update_with_conditions(self, db_ops):
        """Test updating records with conditions."""
        # Create test table
        db_ops.execute_query("""
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
                db_ops.execute_query(
                    "INSERT INTO test_update_conditions VALUES (?, ?, ?, ?)",
                    record
                )
            
            # Update records with conditions
            updates = {"status": "completed", "value": 50.0}
            conditions = {"category": "A"}
            result = db_ops.update_with_conditions("test_update_conditions", updates, conditions)
            assert result == 2
            
            # Verify updates
            results = db_ops.execute_query(
                "SELECT * FROM test_update_conditions WHERE category = 'A'"
            )
            assert len(results) == 2
            assert all(r["status"] == "completed" for r in results)
            assert all(r["value"] == 50.0 for r in results)
            
        finally:
            db_ops.execute_query("DROP TABLE test_update_conditions")

    def test_update_with_conditions_no_match(self, db_ops):
        """Test updating records with conditions that match no records."""
        # Create test table
        db_ops.execute_query("""
            CREATE TABLE test_update_conditions_no_match (
                id INTEGER PRIMARY KEY,
                category TEXT(50)
            )
        """)
        
        try:
            # Insert test record
            db_ops.execute_query(
                "INSERT INTO test_update_conditions_no_match VALUES (?, ?)",
                (1, "A")
            )
            
            # Try to update with non-matching condition
            updates = {"category": "B"}
            conditions = {"category": "C"}
            result = db_ops.update_with_conditions(
                "test_update_conditions_no_match", 
                updates, 
                conditions
            )
            assert result == 0
            
        finally:
            db_ops.execute_query("DROP TABLE test_update_conditions_no_match")

    def test_update_transaction_rollback(self, db_ops):
        """Test transaction rollback during update."""
        # Create test table
        db_ops.execute_query("""
            CREATE TABLE test_update_transaction (
                id INTEGER PRIMARY KEY,
                name TEXT(50) NOT NULL,
                value DECIMAL(10,2)
            )
        """)
        
        try:
            # Insert test record
            db_ops.execute_query(
                "INSERT INTO test_update_transaction VALUES (?, ?, ?)",
                (1, "Test", 10.0)
            )
            
            # Start transaction
            db_ops.begin_transaction()
            
            # Update record
            record = {"id": 1, "name": "Updated", "value": 20.0}
            db_ops.update_record("test_update_transaction", record, ["id"])
            
            # Try to update with invalid data (should fail)
            with pytest.raises(pyodbc.Error):
                invalid_record = {"id": 1, "name": None, "value": 30.0}
                db_ops.update_record("test_update_transaction", invalid_record, ["id"])
            
            # Verify rollback
            results = db_ops.execute_query("SELECT * FROM test_update_transaction")
            assert len(results) == 1
            assert results[0]["name"] == "Test"
            assert results[0]["value"] == 10.0
            
        finally:
            db_ops.execute_query("DROP TABLE test_update_transaction")

    def test_delete_records(self, db_ops, mock_db_path):
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

    def test_soft_delete(self, db_ops, mock_db_path):
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

    def test_cascade_delete(self, db_ops, mock_db_path):
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

    def test_delete_with_transaction(self, db_ops, mock_db_path):
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
            self.db.commit()
            
        except Exception:
            self.db.rollback()
            raise
            
        # Verify deletion after commit
        remaining_records = self.db.read_records(table_name)
        assert len(remaining_records) == 2
        assert all(record["name"] != "Test 2" for record in remaining_records)
        
        # Cleanup
        self.db.drop_table(table_name)

    def test_delete_transaction_rollback(self, db_ops, mock_db_path):
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
            self.db.rollback()
            
        # Verify rollback
        remaining_records = self.db.read_records(table_name)
        assert len(remaining_records) == 3
        assert any(record["name"] == "Test 2" for record in remaining_records)
        
        # Cleanup
        self.db.drop_table(table_name)

    def test_create_table(self, db_ops):
        """Test creating a table with proper Access syntax."""
        columns = {
            'id': 'INTEGER',
            'name': 'TEXT',
            'value': 'DECIMAL',
            'created_date': 'DATETIME'
        }
        
        try:
            db_ops.create_table('test_create', columns, ['id'])
            
            # Verify table was created
            tables = db_ops.get_tables()
            assert 'test_create' in tables
            
            # Verify column types
            columns = db_ops.get_table_columns('test_create')
            assert 'id' in columns
            assert 'name' in columns
            assert 'value' in columns
            assert 'created_date' in columns
            
        finally:
            db_ops.execute_query('DROP TABLE test_create')
            
    def test_alter_table(self, db_ops):
        """Test altering a table with proper Access syntax."""
        # Create initial table
        columns = {
            'id': 'INTEGER',
            'name': 'TEXT'
        }
        db_ops.create_table('test_alter', columns, ['id'])
        
        try:
            # Add new column
            add_columns = {
                'value': 'DECIMAL',
                'created_date': 'DATETIME'
            }
            db_ops.alter_table('test_alter', add_columns=add_columns)
            
            # Verify new columns
            columns = db_ops.get_table_columns('test_alter')
            assert 'value' in columns
            assert 'created_date' in columns
            
            # Drop column
            db_ops.alter_table('test_alter', drop_columns=['value'])
            
            # Verify column was dropped
            columns = db_ops.get_table_columns('test_alter')
            assert 'value' not in columns
            
        finally:
            db_ops.execute_query('DROP TABLE test_alter')
            
    def test_add_foreign_key(self, db_ops):
        """Test adding a foreign key constraint."""
        # Create parent table
        db_ops.create_table(
            "test_parent",
            ["id INTEGER PRIMARY KEY", "name TEXT"],
            primary_keys=["id"]
        )
        
        # Create child table
        db_ops.create_table(
            "test_child",
            ["id INTEGER PRIMARY KEY", "parent_id INTEGER", "value TEXT"],
            primary_keys=["id"]
        )
        
        # Add foreign key constraint
        db_ops.add_foreign_key(
            "test_child",
            "parent_id",
            "test_parent",
            "id"
        )
        
        # Insert valid record
        db_ops.insert_record("test_parent", {"id": 1, "name": "Parent 1"})
        db_ops.insert_record("test_child", {"id": 1, "parent_id": 1, "value": "Child 1"})
        
        # Verify foreign key constraint
        with pytest.raises(Exception):
            db_ops.insert_record("test_child", {"id": 2, "parent_id": 999, "value": "Invalid"})
        
        # Cleanup
        db_ops.drop_table("test_child")
        db_ops.drop_table("test_parent")

    def test_read_records_with_limit(self, db_ops):
        """Test reading records with LIMIT using Access TOP syntax."""
        # Create test table
        columns = {
            'id': 'INTEGER',
            'name': 'TEXT',
            'value': 'DECIMAL'
        }
        db_ops.create_table('test_limit', columns, ['id'])
        
        try:
            # Insert test data
            for i in range(10):
                db_ops.insert_data('test_limit', {
                    'id': i + 1,
                    'name': f'Test {i + 1}',
                    'value': float(i + 1)
                })
            
            # Test with limit
            results = db_ops.read_records('test_limit', limit=5)
            assert len(results) == 5
            
            # Test with limit and offset (should raise NotImplementedError)
            with pytest.raises(NotImplementedError):
                db_ops.read_records('test_limit', limit=5, offset=5)
                
        finally:
            db_ops.execute_query('DROP TABLE test_limit')
            
    def test_update_record(self, db_ops):
        """Test updating a record with proper Access syntax."""
        # Create test table
        columns = {
            'id': 'INTEGER',
            'name': 'TEXT',
            'value': 'DECIMAL',
            'status': 'TEXT'
        }
        db_ops.create_table('test_update', columns, ['id'])
        
        try:
            # Insert test record
            db_ops.insert_data('test_update', {
                'id': 1,
                'name': 'Test',
                'value': 10.5,
                'status': 'active'
            })
            
            # Update record
            result = db_ops.update_data(
                'test_update',
                {'name': 'Updated', 'value': 20.75, 'status': 'inactive'},
                'id = ?',
                {'id': 1}
            )
            assert result == 1
            
            # Verify update
            results = db_ops.read_records('test_update', {'id': 1})
            assert len(results) == 1
            assert results[0]['name'] == 'Updated'
            assert results[0]['value'] == 20.75
            assert results[0]['status'] == 'inactive'
            
        finally:
            db_ops.execute_query('DROP TABLE test_update')
            
    def test_delete_data(self, db_ops):
        """Test deleting data with proper Access syntax."""
        # Create test table
        columns = {
            'id': 'INTEGER',
            'name': 'TEXT',
            'value': 'DECIMAL'
        }
        db_ops.create_table('test_delete', columns, ['id'])
        
        try:
            # Insert test data
            for i in range(5):
                db_ops.insert_data('test_delete', {
                    'id': i + 1,
                    'name': f'Test {i + 1}',
                    'value': float(i + 1)
                })
            
            # Delete records
            result = db_ops.delete_data(
                'test_delete',
                'value > ?',
                {'value': 3}
            )
            assert result == 2
            
            # Verify deletion
            results = db_ops.read_records('test_delete')
            assert len(results) == 3
            
        finally:
            db_ops.execute_query('DROP TABLE test_delete')

    def test_get_tables(self):
        """Test getting list of tables."""
        # Create test tables
        self.db.execute_query("""
            CREATE TABLE test_table1 (
                id LONG PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        self.db.execute_query("""
            CREATE TABLE test_table2 (
                id LONG PRIMARY KEY,
                value DOUBLE
            )
        """)
        
        # Get tables
        tables = self.db.get_tables()
        
        # Verify results
        self.assertIn('test_table1', tables)
        self.assertIn('test_table2', tables)
        
        # Cleanup
        self.db.drop_table('test_table1')
        self.db.drop_table('test_table2')

    def test_get_table_columns(self):
        """Test getting table columns."""
        # Create test table
        self.db.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY,
                name VARCHAR(255),
                value DOUBLE
            )
        """)
        
        # Get columns
        columns = self.db.get_table_columns('test_table')
        
        # Verify results
        self.assertEqual(['id', 'name', 'value'], columns)
        
        # Cleanup
        self.db.drop_table('test_table')

    def test_table_exists(self):
        """Test table existence check."""
        # Create test table
        self.db.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY
            )
        """)
        
        # Check existence
        self.assertTrue(self.db.table_exists('test_table'))
        self.assertFalse(self.db.table_exists('nonexistent_table'))
        
        # Cleanup
        self.db.drop_table('test_table')

    def test_drop_table(self):
        """Test dropping a table."""
        # Create test table
        self.db.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY
            )
        """)
        
        # Verify table exists
        self.assertTrue(self.db.table_exists('test_table'))
        
        # Drop table
        self.db.drop_table('test_table')
        
        # Verify table no longer exists
        self.assertFalse(self.db.table_exists('test_table'))
        
        # Test dropping non-existent table
        with self.assertRaises(DatabaseError):
            self.db.drop_table('nonexistent_table')

    def test_truncate_table(self):
        """Test truncating a table."""
        # Create and populate test table
        self.db.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY,
                value DOUBLE
            )
        """)
        self.db.execute_query(
            "INSERT INTO test_table (id, value) VALUES (?, ?)",
            (1, 10.0)
        )
        
        # Verify record exists
        count_before = self.db.execute_query(
            "SELECT COUNT(*) as count FROM test_table"
        )[0]['count']
        self.assertEqual(1, count_before)
        
        # Truncate table
        self.db.truncate_table('test_table')
        
        # Verify table is empty
        count_after = self.db.execute_query(
            "SELECT COUNT(*) as count FROM test_table"
        )[0]['count']
        self.assertEqual(0, count_after)
        
        # Cleanup
        self.db.drop_table('test_table')

    def test_get_table_info(self):
        """Test getting table information."""
        # Create test table
        self.db.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY,
                name VARCHAR(255),
                value DOUBLE
            )
        """)
        self.db.execute_query(
            "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
            (1, 'test', 10.0)
        )
        
        # Get table info
        info = self.db.get_table_info('test_table')
        
        # Verify results
        self.assertEqual('test_table', info['name'])
        self.assertEqual(1, info['record_count'])
        self.assertEqual(3, len(info['columns']))
        
        # Verify column information
        column_names = [col['name'] for col in info['columns']]
        self.assertEqual(['id', 'name', 'value'], column_names)
        
        # Cleanup
        self.db.drop_table('test_table')

    def test_verify_record_exists(self):
        """Test record existence verification."""
        # Create and populate test table
        self.db.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        self.db.execute_query(
            "INSERT INTO test_table (id, name) VALUES (?, ?)",
            (1, 'test')
        )
        
        # Test existing record
        self.assertTrue(self.db.verify_record_exists(
            'test_table',
            {'id': 1, 'name': 'test'}
        ))
        
        # Test non-existing record
        self.assertFalse(self.db.verify_record_exists(
            'test_table',
            {'id': 2, 'name': 'nonexistent'}
        ))
        
        # Cleanup
        self.db.drop_table('test_table')

    def test_transaction_management(self):
        """Test transaction management."""
        # Create test table
        self.db.execute_query("""
            CREATE TABLE test_table (
                id LONG PRIMARY KEY,
                value DOUBLE
            )
        """)
        
        # Test successful transaction
        self.db.begin_transaction()
        self.db.execute_query(
            "INSERT INTO test_table (id, value) VALUES (?, ?)",
            (1, 10.0)
        )
        self.db.commit()
        
        # Verify record was committed
        count = self.db.execute_query(
            "SELECT COUNT(*) as count FROM test_table"
        )[0]['count']
        self.assertEqual(1, count)
        
        # Test transaction rollback
        self.db.begin_transaction()
        self.db.execute_query(
            "INSERT INTO test_table (id, value) VALUES (?, ?)",
            (2, 20.0)
        )
        self.db.rollback()
        
        # Verify record was not committed
        count = self.db.execute_query(
            "SELECT COUNT(*) as count FROM test_table"
        )[0]['count']
        self.assertEqual(1, count)
        
        # Cleanup
        self.db.drop_table('test_table')

    def test_context_manager(self):
        """Test context manager functionality."""
        with self.db as db:
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
        self.assertIsNone(self.db.conn)
        self.assertIsNone(self.db.cursor) 