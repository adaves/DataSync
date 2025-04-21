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
from ..fixtures.mock_database.create_mock_db import create_mock_database

@pytest.fixture(scope="session")
def mock_db_template():
    """Create a template mock database that can be copied for individual tests."""
    template_path = Path(__file__).parent / "mock_database" / "mock_database.accdb"
    if not template_path.exists():
        create_mock_database(str(template_path))
    return template_path

@pytest.fixture
def temp_db_path(tmp_path, mock_db_template):
    """
    Create a temporary database path with a fresh copy of the mock database.
    
    Args:
        tmp_path: pytest fixture for temporary directory
        mock_db_template: Path to the template mock database
        
    Returns:
        Path to the temporary database file
    """
    temp_db = tmp_path / "test_database.accdb"
    shutil.copy(mock_db_template, temp_db)
    return temp_db

@pytest.fixture
def db_operations(temp_db_path):
    """
    Create a DatabaseOperations instance with a fresh copy of the mock database.
    
    Args:
        temp_db_path: Path to the temporary database file
        
    Returns:
        DatabaseOperations instance
    """
    ops = DatabaseOperations(temp_db_path)
    yield ops
    # Cleanup: close any open connections
    if ops.conn:
        ops.close()

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
def module_db_path(tmp_path_factory, mock_db_template):
    """
    Create a temporary database path that persists for all tests in a module.
    
    Args:
        tmp_path_factory: pytest fixture for creating temporary directories
        mock_db_template: Path to the template mock database
        
    Returns:
        Path to the temporary database file
    """
    temp_dir = tmp_path_factory.mktemp("module_db")
    temp_db = temp_dir / "module_database.accdb"
    shutil.copy(mock_db_template, temp_db)
    return temp_db

@pytest.fixture(scope="module")
def module_db_operations(module_db_path):
    """
    Create a DatabaseOperations instance that persists for all tests in a module.
    
    Args:
        module_db_path: Path to the module-level temporary database
        
    Returns:
        DatabaseOperations instance
    """
    ops = DatabaseOperations(module_db_path)
    yield ops
    # Cleanup: close any open connections
    if ops.conn:
        ops.close()

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