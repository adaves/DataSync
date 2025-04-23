"""
Test configuration and fixtures.
"""

import os
import pytest
import pandas as pd
from pathlib import Path
from typing import Generator, Dict, Any
from datasync.database.operations import DatabaseOperations
from datasync.processing.excel_processor import ExcelProcessor
from datasync.processing.file_manager import FileManager
from datasync.utils.config import load_config

@pytest.fixture(scope="session")
def test_config() -> Dict[str, Any]:
    """Load test configuration from .env.test file."""
    from dotenv import load_dotenv
    load_dotenv(".env.test")
    return {
        "db_path": os.getenv("TEST_DB_PATH"),
        "db_connection_string": os.getenv("TEST_DB_CONNECTION_STRING"),
        "data_dir": os.getenv("TEST_DATA_DIR"),
        "output_dir": os.getenv("TEST_OUTPUT_DIR"),
        "temp_dir": os.getenv("TEST_TEMP_DIR"),
        "log_level": os.getenv("TEST_LOG_LEVEL"),
        "log_file": os.getenv("TEST_LOG_FILE"),
        "batch_size": int(os.getenv("TEST_BATCH_SIZE", "1000")),
        "concurrent_threads": int(os.getenv("TEST_CONCURRENT_THREADS", "4")),
        "timeout_seconds": int(os.getenv("TEST_TIMEOUT_SECONDS", "30")),
    }

@pytest.fixture(scope="session")
def db_operations(test_config) -> Generator[DatabaseOperations, None, None]:
    """Create a database operations instance for testing."""
    ops = DatabaseOperations(test_config["db_connection_string"])
    yield ops
    ops.close()

@pytest.fixture(scope="session")
def excel_processor(test_config) -> Generator[ExcelProcessor, None, None]:
    """Create an Excel processor instance for testing."""
    sample_file = Path(test_config["data_dir"]) / "simple_data.xlsx"
    processor = ExcelProcessor(sample_file)
    yield processor

@pytest.fixture(scope="session")
def file_manager(test_config) -> Generator[FileManager, None, None]:
    """Create a file manager instance for testing."""
    manager = FileManager()
    yield manager

@pytest.fixture(scope="function")
def temp_dir(test_config) -> Generator[Path, None, None]:
    """Create a temporary directory for testing."""
    temp_path = Path(test_config["temp_dir"])
    temp_path.mkdir(parents=True, exist_ok=True)
    yield temp_path
    # Cleanup
    for file in temp_path.glob("*"):
        if file.is_file():
            file.unlink()
        elif file.is_dir():
            for subfile in file.glob("**/*"):
                if subfile.is_file():
                    subfile.unlink()
            file.rmdir()
    temp_path.rmdir()

@pytest.fixture(scope="function")
def sample_dataframe() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'active': [True, False, True],
        'created_at': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03'])
    })

@pytest.fixture(scope="function")
def mock_database(test_config) -> Generator[Path, None, None]:
    """Create a mock database for testing."""
    db_path = Path(test_config["db_path"])
    if not db_path.exists():
        # Create mock database if it doesn't exist
        from datasync.database.operations import DatabaseOperations
        ops = DatabaseOperations(test_config["db_connection_string"])
        ops.create_table("test_table", {
            "id": "INTEGER PRIMARY KEY",
            "name": "TEXT",
            "age": "INTEGER",
            "active": "BOOLEAN",
            "created_at": "DATETIME"
        })
        ops.close()
    yield db_path
    # No cleanup needed as we want to keep the mock database

@pytest.fixture(scope="function")
def mock_excel_file(test_config, sample_dataframe) -> Generator[Path, None, None]:
    """Create a mock Excel file for testing."""
    output_dir = Path(test_config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / "test_data.xlsx"
    sample_dataframe.to_excel(file_path, index=False)
    yield file_path
    if file_path.exists():
        file_path.unlink() 