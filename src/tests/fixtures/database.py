"""
Database test fixtures for the DataSync application.
This module provides fixtures for creating and managing test databases.
"""

import pytest
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from datasync.database.operations import DatabaseOperations
from datasync.database.validation import DatabaseValidation
from datasync.database.monitoring import DatabaseMonitor
from datasync.utils.path_utils import ensure_directory_exists
from datasync.utils.logger import setup_logger
from tests.fixtures.mock_database.create_mock_db import create_mock_database
import os

@pytest.fixture(scope="session")
def mock_db_template():
    """Create a template mock database that can be copied for individual tests."""
    template_path = Path(__file__).parent / "mock_database" / "mock_database.accdb"
    if not template_path.exists():
        create_mock_database(str(template_path))
    return template_path

@pytest.fixture
def mock_db_path(tmp_path):
    """
    Create a temporary path for the test database.
    
    Args:
        tmp_path: pytest's temporary directory fixture
        
    Returns:
        Path to the temporary database file
    """
    return tmp_path / "test.accdb"

@pytest.fixture
def db_operations(mock_db_path):
    """
    Create a DatabaseOperations instance with a temporary database.
    
    Args:
        mock_db_path: Path to the temporary database file
        
    Returns:
        DatabaseOperations instance
    """
    # Create parent directory if it doesn't exist
    if not os.path.exists(os.path.dirname(mock_db_path)):
        os.makedirs(os.path.dirname(mock_db_path), exist_ok=True)
        
    # Create and initialize the database
    create_mock_database(str(mock_db_path))
    
    ops = DatabaseOperations(mock_db_path)
    ops.connect()  # Ensure connection is established
    
    yield ops
    
    try:
        if ops.in_transaction:
            ops.rollback_transaction()
        ops.close()
    except Exception:
        pass  # Ignore cleanup errors
    finally:
        try:
            if os.path.exists(mock_db_path):
                os.remove(mock_db_path)
        except Exception:
            pass  # Ignore file deletion errors

@pytest.fixture
def db_validation():
    """
    Create a DatabaseValidation instance.
    
    Returns:
        DatabaseValidation instance
    """
    return DatabaseValidation()

@pytest.fixture
def db_monitor():
    """
    Create a DatabaseMonitor instance.
    
    Returns:
        DatabaseMonitor instance
    """
    return DatabaseMonitor()

@pytest.fixture(scope="module")
def module_db_path(tmp_path_factory):
    """
    Create a module-scoped temporary path for the test database.
    
    Args:
        tmp_path_factory: pytest's temporary directory factory fixture
        
    Returns:
        Path to the temporary database file
    """
    tmp_dir = tmp_path_factory.mktemp("db")
    return tmp_dir / "test.accdb"

@pytest.fixture(scope="module")
def module_db_operations(module_db_path):
    """
    Create a module-scoped DatabaseOperations instance.
    
    Args:
        module_db_path: Path to the module-level temporary database
        
    Returns:
        DatabaseOperations instance
    """
    # Create parent directory if it doesn't exist
    if not os.path.exists(os.path.dirname(module_db_path)):
        os.makedirs(os.path.dirname(module_db_path), exist_ok=True)
        
    # Create an empty database file
    if not os.path.exists(module_db_path):
        with open(module_db_path, 'wb') as f:
            # Write empty Access database file header
            f.write(b'\x00' * 4096)
    
    ops = DatabaseOperations(module_db_path)
    ops.connect()  # Ensure connection is established
    
    yield ops
    
    try:
        if ops.in_transaction:
            ops.rollback_transaction()
        ops.close()
    except Exception:
        pass  # Ignore cleanup errors
    finally:
        try:
            if os.path.exists(module_db_path):
                os.remove(module_db_path)
        except Exception:
            pass  # Ignore file deletion errors

@pytest.fixture
def sample_records():
    """
    Create sample records for testing.
    
    Returns:
        Dictionary containing sample records for different tables
    """
    base_date = datetime.now()
    return {
        'test_table': [
            {'id': 1, 'name': 'Test 1', 'value': 10.5, 'created_at': base_date},
            {'id': 2, 'name': 'Test 2', 'value': 20.5, 'created_at': base_date + timedelta(days=1)},
            {'id': 3, 'name': 'Test 3', 'value': 30.5, 'created_at': base_date + timedelta(days=2)}
        ],
        'yearly_data': [
            {'id': 1, 'value': 100, 'time': datetime(2022, 1, 1)},
            {'id': 2, 'value': 200, 'time': datetime(2022, 6, 1)},
            {'id': 3, 'value': 300, 'time': datetime(2023, 1, 1)},
            {'id': 4, 'value': 400, 'time': datetime(2023, 6, 1)}
        ]
    }

@pytest.fixture
def setup_test_table(db_operations):
    """
    Set up a test table with sample data.
    
    Args:
        db_operations: DatabaseOperations instance
        
    Returns:
        Tuple of (table_name, sample_records)
    """
    table_name = 'test_table'
    
    # Create table
    db_operations.execute_query("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT(50),
            value DOUBLE,
            created_at DATETIME
        )
    """)
    
    # Insert sample records
    records = [
        {'id': 1, 'name': 'Test 1', 'value': 10.5, 'created_at': datetime.now()},
        {'id': 2, 'name': 'Test 2', 'value': 20.5, 'created_at': datetime.now()},
        {'id': 3, 'name': 'Test 3', 'value': 30.5, 'created_at': datetime.now()}
    ]
    
    for record in records:
        db_operations.insert_record(table_name, record)
    
    yield table_name, records
    
    # Cleanup
    db_operations.execute_query(f"DROP TABLE {table_name}")

@pytest.fixture
def setup_yearly_data(db_operations):
    """
    Set up a table with yearly data for testing.
    
    Args:
        db_operations: DatabaseOperations instance
        
    Returns:
        Tuple of (table_name, sample_records)
    """
    table_name = 'yearly_data'
    
    # Create table
    db_operations.execute_query("""
        CREATE TABLE yearly_data (
            id INTEGER PRIMARY KEY,
            value DOUBLE,
            time DATETIME
        )
    """)
    
    # Insert sample records
    records = [
        {'id': 1, 'value': 100, 'time': datetime(2022, 1, 1)},
        {'id': 2, 'value': 200, 'time': datetime(2022, 6, 1)},
        {'id': 3, 'value': 300, 'time': datetime(2023, 1, 1)},
        {'id': 4, 'value': 400, 'time': datetime(2023, 6, 1)}
    ]
    
    for record in records:
        db_operations.insert_record(table_name, record)
    
    yield table_name, records
    
    # Cleanup
    db_operations.execute_query(f"DROP TABLE {table_name}")

@pytest.fixture
def setup_transaction_test(db_operations):
    """
    Set up a table for transaction testing.
    
    Args:
        db_operations: DatabaseOperations instance
        
    Returns:
        Tuple of (table_name, sample_records)
    """
    table_name = 'transaction_test'
    
    # Create table
    db_operations.execute_query("""
        CREATE TABLE transaction_test (
            id INTEGER PRIMARY KEY,
            name TEXT(50) NOT NULL,
            value DOUBLE
        )
    """)
    
    # Insert sample records
    records = [
        {'id': 1, 'name': 'Test 1', 'value': 10.5},
        {'id': 2, 'name': 'Test 2', 'value': 20.5}
    ]
    
    for record in records:
        db_operations.insert_record(table_name, record)
    
    yield table_name, records
    
    # Cleanup
    db_operations.execute_query(f"DROP TABLE {table_name}") 