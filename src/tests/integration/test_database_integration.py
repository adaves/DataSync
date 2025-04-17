"""
Integration tests for database operations.
This module contains tests that verify the interaction between different components
and test real database operations with actual Microsoft Access databases.
"""

import pytest
import os
from pathlib import Path
from datetime import datetime
import pyodbc
from datasync.database.operations import DatabaseOperations
from datasync.database.validation import DatabaseValidation
from datasync.database.monitoring import DatabaseMonitor

@pytest.fixture(scope="module")
def test_db_path(tmp_path_factory):
    """Create a temporary test database that persists for all tests in this module."""
    db_path = tmp_path_factory.mktemp("test_db") / "test_database.accdb"
    return db_path

@pytest.fixture(scope="module")
def db_operations(test_db_path):
    """Create a DatabaseOperations instance with the test database."""
    return DatabaseOperations(test_db_path)

@pytest.fixture(scope="module")
def db_validation():
    """Create a DatabaseValidation instance."""
    return DatabaseValidation()

@pytest.fixture(scope="module")
def db_monitor():
    """Create a DatabaseMonitor instance."""
    return DatabaseMonitor()

class TestDatabaseIntegration:
    """Test suite for database integration tests."""
    
    def test_database_connection(self, db_operations):
        """Test establishing a real database connection."""
        try:
            db_operations.connect()
            assert db_operations.conn is not None
            assert db_operations.cursor is not None
        finally:
            db_operations.close()
    
    def test_create_and_query_table(self, db_operations):
        """Test creating a table and performing basic queries."""
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
            
            # Insert some test data
            test_data = [
                ("Test1", 42.5, datetime.now()),
                ("Test2", 99.9, datetime.now())
            ]
            
            for name, value, date in test_data:
                insert_sql = """
                INSERT INTO TestTable (Name, Value, CreatedDate)
                VALUES (?, ?, ?)
                """
                db_operations.execute_query(insert_sql, (name, value, date))
            
            # Query the data
            select_sql = "SELECT * FROM TestTable ORDER BY ID"
            results = db_operations.execute_query(select_sql)
            
            assert len(results) == 2
            assert results[0]["Name"] == "Test1"
            assert results[0]["Value"] == 42.5
            assert results[1]["Name"] == "Test2"
            assert results[1]["Value"] == 99.9
            
        finally:
            # Clean up
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestTable")
                db_operations.close()
    
    def test_transaction_rollback(self, db_operations):
        """Test transaction rollback on error."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestTransactions (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50) NOT NULL
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Start a transaction
            db_operations.execute_query("BEGIN TRANSACTION")
            
            # Insert valid data
            db_operations.execute_query(
                "INSERT INTO TestTransactions (Name) VALUES (?)",
                ("Valid",)
            )
            
            # Try to insert invalid data (should fail)
            with pytest.raises(pyodbc.Error):
                db_operations.execute_query(
                    "INSERT INTO TestTransactions (Name) VALUES (?)",
                    (None,)  # This should fail due to NOT NULL constraint
                )
            
            # Rollback the transaction
            db_operations.execute_query("ROLLBACK")
            
            # Verify no data was inserted
            results = db_operations.execute_query("SELECT * FROM TestTransactions")
            assert len(results) == 0
            
        finally:
            # Clean up
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestTransactions")
                db_operations.close()
    
    def test_concurrent_operations(self, db_operations):
        """Test handling of concurrent database operations."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestConcurrency (
                ID COUNTER PRIMARY KEY,
                Counter INT DEFAULT 0
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Insert initial data
            db_operations.execute_query(
                "INSERT INTO TestConcurrency (Counter) VALUES (0)"
            )
            
            # Simulate concurrent updates
            update_sql = """
            UPDATE TestConcurrency
            SET Counter = Counter + 1
            WHERE ID = 1
            """
            
            # Perform multiple updates
            for _ in range(5):
                db_operations.execute_query(update_sql)
            
            # Verify final state
            results = db_operations.execute_query(
                "SELECT Counter FROM TestConcurrency WHERE ID = 1"
            )
            assert results[0]["Counter"] == 5
            
        finally:
            # Clean up
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestConcurrency")
                db_operations.close()
    
    def test_error_handling(self, db_operations):
        """Test error handling with invalid operations."""
        try:
            db_operations.connect()
            
            # Test invalid table name
            with pytest.raises(pyodbc.Error):
                db_operations.execute_query("SELECT * FROM NonExistentTable")
            
            # Test invalid SQL syntax
            with pytest.raises(pyodbc.Error):
                db_operations.execute_query("INVALID SQL STATEMENT")
            
            # Test invalid parameter count
            with pytest.raises(pyodbc.Error):
                db_operations.execute_query(
                    "SELECT * FROM TestTable WHERE ID = ?",
                    (1, 2)  # Too many parameters
                )
            
        finally:
            if db_operations.conn:
                db_operations.close()

    def test_insert_record_integration(self, db_operations):
        """Test insert_record with a real database."""
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
            
        finally:
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestInsert")
                db_operations.close()

    def test_batch_insert_integration(self, db_operations):
        """Test batch_insert with a real database."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestBatchInsert (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50),
                Value DOUBLE,
                CreatedDate DATETIME
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Prepare test data
            records = [
                {
                    "Name": f"Test {i}",
                    "Value": float(i),
                    "CreatedDate": datetime.now()
                }
                for i in range(1, 6)  # 5 records
            ]
            
            # Test batch insertion
            result = db_operations.batch_insert("TestBatchInsert", records, batch_size=2)
            assert result == 5
            
            # Verify all records were inserted
            results = db_operations.execute_query("SELECT * FROM TestBatchInsert ORDER BY ID")
            assert len(results) == 5
            for i, record in enumerate(results, 1):
                assert record["Name"] == f"Test {i}"
                assert record["Value"] == float(i)
            
        finally:
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestBatchInsert")
                db_operations.close()

    def test_upsert_integration(self, db_operations):
        """Test upsert with a real database."""
        try:
            db_operations.connect()
            
            # Create a test table with a unique constraint
            create_table_sql = """
            CREATE TABLE TestUpsert (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50) UNIQUE,
                Value DOUBLE,
                CreatedDate DATETIME
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Test initial insert
            record = {
                "Name": "Test Upsert",
                "Value": 42.5,
                "CreatedDate": datetime.now()
            }
            
            result = db_operations.upsert("TestUpsert", record, ["Name"])
            assert result == 1
            
            # Verify initial insert
            results = db_operations.execute_query("SELECT * FROM TestUpsert")
            assert len(results) == 1
            assert results[0]["Value"] == 42.5
            
            # Test update
            updated_record = {
                "Name": "Test Upsert",  # Same name to trigger update
                "Value": 99.9,          # Different value
                "CreatedDate": datetime.now()
            }
            
            result = db_operations.upsert("TestUpsert", updated_record, ["Name"])
            assert result == 1
            
            # Verify update
            results = db_operations.execute_query("SELECT * FROM TestUpsert")
            assert len(results) == 1
            assert results[0]["Value"] == 99.9
            
        finally:
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestUpsert")
                db_operations.close()

    def test_transaction_integration(self, db_operations):
        """Test transaction management with a real database."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestTransactions (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50),
                Value DOUBLE
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Test transaction rollback
            db_operations.begin_transaction()
            
            # Insert a record
            record = {
                "Name": "Test Transaction",
                "Value": 42.5
            }
            db_operations.insert_record("TestTransactions", record)
            
            # Verify record exists before rollback
            results = db_operations.execute_query("SELECT * FROM TestTransactions")
            assert len(results) == 1
            
            # Rollback the transaction
            db_operations.rollback_transaction()
            
            # Verify record was rolled back
            results = db_operations.execute_query("SELECT * FROM TestTransactions")
            assert len(results) == 0
            
            # Test successful transaction
            db_operations.begin_transaction()
            
            # Insert a record
            db_operations.insert_record("TestTransactions", record)
            
            # Commit the transaction
            db_operations.commit_transaction()
            
            # Verify record exists after commit
            results = db_operations.execute_query("SELECT * FROM TestTransactions")
            assert len(results) == 1
            assert results[0]["Name"] == "Test Transaction"
            assert results[0]["Value"] == 42.5
            
        finally:
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestTransactions")
                db_operations.close() 