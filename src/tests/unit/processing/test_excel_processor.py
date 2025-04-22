"""
Unit tests for Excel processing functionality.
"""

import pytest
import pandas as pd
import os
from pathlib import Path
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import threading
from datasync.processing.excel_processor import ExcelProcessor

@pytest.fixture
def sample_excel_file(tmp_path):
    """Create a sample Excel file for testing."""
    file_path = tmp_path / "test.xlsx"
    
    # Create sample data
    df1 = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [10, 20, 30]
    })
    
    df2 = pd.DataFrame({
        'id': [4, 5, 6],
        'description': ['X', 'Y', 'Z'],
        'amount': [100, 200, 300]
    })
    
    # Write to Excel file
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        df1.to_excel(writer, sheet_name='Sheet1', index=False)
        df2.to_excel(writer, sheet_name='Sheet2', index=False)
    
    return file_path

@pytest.fixture
def large_excel_file(tmp_path):
    """Create a large Excel file for testing."""
    file_path = tmp_path / "large_test.xlsx"
    
    # Create large sample data (10,000 rows instead of 100,000 to avoid memory issues)
    large_df = pd.DataFrame({
        'id': range(10000),
        'name': [f'Name_{i}' for i in range(10000)],
        'value': np.random.randint(1, 1000, 10000),
        'date': pd.date_range('2024-01-01', periods=10000, freq='h'),  # Hourly intervals
        'category': np.random.choice(['A', 'B', 'C', 'D'], 10000)
    })
    
    # Write to Excel file
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        large_df.to_excel(writer, sheet_name='LargeSheet', index=False)
    
    return file_path

@pytest.fixture
def invalid_data_excel(tmp_path):
    """Create an Excel file with invalid data for testing."""
    file_path = tmp_path / "invalid_test.xlsx"
    
    # Create data with various issues
    df = pd.DataFrame({
        'id': ['1', '2', 'not_a_number', '4'],
        'email': ['valid@email.com', 'invalid_email', 'another@email.com', 'not_an_email'],
        'date': ['2024-01-01', 'invalid_date', '2024-01-03', '2024-13-45'],
        'amount': [100, -50, 'invalid', 200]
    })
    
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='InvalidData', index=False)
    
    return file_path

# Basic Tests
def test_excel_processor_initialization(sample_excel_file):
    """Test ExcelProcessor initialization."""
    processor = ExcelProcessor(sample_excel_file)
    assert processor.file_path == Path(sample_excel_file)

def test_excel_processor_invalid_file():
    """Test ExcelProcessor with invalid file."""
    with pytest.raises(FileNotFoundError):
        ExcelProcessor("nonexistent.xlsx")

def test_excel_processor_invalid_file_type(tmp_path):
    """Test ExcelProcessor with invalid file type."""
    file_path = tmp_path / "test.txt"
    file_path.touch()
    with pytest.raises(ValueError):
        ExcelProcessor(file_path)

# Data Reading Tests
def test_read_sheet(sample_excel_file):
    """Test reading a specific sheet."""
    processor = ExcelProcessor(sample_excel_file)
    df = processor.read_sheet('Sheet1')
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ['id', 'name', 'value']

def test_read_all_sheets(sample_excel_file):
    """Test reading all sheets."""
    processor = ExcelProcessor(sample_excel_file)
    sheets = processor.read_all_sheets()
    assert isinstance(sheets, dict)
    assert len(sheets) == 2
    assert 'Sheet1' in sheets
    assert 'Sheet2' in sheets

# Data Writing Tests
def test_write_sheet(sample_excel_file):
    """Test writing to a sheet."""
    processor = ExcelProcessor(sample_excel_file)
    new_data = pd.DataFrame({
        'id': [7, 8, 9],
        'name': ['D', 'E', 'F'],
        'value': [40, 50, 60]
    })
    processor.write_sheet(new_data, 'Sheet3')
    
    # Verify the new sheet was written
    sheets = processor.read_all_sheets()
    assert 'Sheet3' in sheets
    assert len(sheets['Sheet3']) == 3

# Sheet Management Tests
def test_get_sheet_names(sample_excel_file):
    """Test getting sheet names."""
    processor = ExcelProcessor(sample_excel_file)
    sheet_names = processor.get_sheet_names()
    assert isinstance(sheet_names, list)
    assert len(sheet_names) == 2
    assert 'Sheet1' in sheet_names
    assert 'Sheet2' in sheet_names

# Data Validation Tests
def test_validate_sheet_structure(sample_excel_file):
    """Test sheet structure validation."""
    processor = ExcelProcessor(sample_excel_file)
    
    # Test with valid columns
    assert processor.validate_sheet_structure('Sheet1', ['id', 'name', 'value'])
    
    # Test with missing columns
    assert not processor.validate_sheet_structure('Sheet1', ['id', 'name', 'value', 'extra'])

def test_validate_data_types(invalid_data_excel):
    """Test data type validation."""
    processor = ExcelProcessor(invalid_data_excel)
    df = processor.read_sheet('InvalidData')
    
    # Test numeric validation
    numeric_mask = pd.to_numeric(df['id'], errors='coerce').notna()
    assert not numeric_mask.all()
    
    # Test email validation
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    email_mask = df['email'].str.match(email_pattern)
    assert not email_mask.all()
    
    # Test date validation
    date_mask = pd.to_datetime(df['date'], errors='coerce').notna()
    assert not date_mask.all()

# Data Transformation Tests
def test_data_transformation(sample_excel_file):
    """Test data transformation capabilities."""
    processor = ExcelProcessor(sample_excel_file)
    df = processor.read_sheet('Sheet1')
    
    # Test numeric transformations
    df['value_doubled'] = df['value'] * 2
    assert (df['value_doubled'] == df['value'] * 2).all()
    
    # Test string transformations
    df['name_upper'] = df['name'].str.upper()
    assert (df['name_upper'] == df['name'].str.upper()).all()
    
    # Test calculated columns
    df['value_category'] = pd.cut(df['value'], bins=[0, 15, 25, 35], labels=['Low', 'Medium', 'High'])
    assert set(df['value_category'].dropna()) <= {'Low', 'Medium', 'High'}

# Large File Handling Tests
def test_large_file_handling(large_excel_file):
    """Test handling of large Excel files."""
    processor = ExcelProcessor(large_excel_file)
    
    # Test reading large file
    df = processor.read_sheet('LargeSheet')
    assert len(df) == 10000  # Updated to match new size
    assert all(col in df.columns for col in ['id', 'name', 'value', 'date', 'category'])
    
    # Test memory efficiency
    import psutil
    process = psutil.Process()
    memory_before = process.memory_info().rss
    
    # Perform operations on large dataset
    df['value_sum'] = df['value'].cumsum()
    df['category_count'] = df.groupby('category')['id'].transform('count')
    
    memory_after = process.memory_info().rss
    memory_increase = (memory_after - memory_before) / 1024 / 1024  # MB
    
    # Assert memory increase is reasonable (less than 500MB)
    assert memory_increase < 500

# Concurrent Processing Tests
def test_concurrent_processing(sample_excel_file):
    """Test concurrent Excel file operations."""
    processor = ExcelProcessor(sample_excel_file)
    results = []
    errors = []
    
    def read_sheet_concurrent(sheet_name):
        try:
            df = processor.read_sheet(sheet_name)
            results.append((sheet_name, len(df)))
        except Exception as e:
            errors.append((sheet_name, str(e)))
    
    # Test concurrent sheet reading
    sheet_names = processor.get_sheet_names()
    threads = []
    for sheet_name in sheet_names:
        thread = threading.Thread(target=read_sheet_concurrent, args=(sheet_name,))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    assert len(results) == len(sheet_names)
    assert not errors

def test_error_handling(invalid_data_excel):
    """Test error handling for various scenarios."""
    processor = ExcelProcessor(invalid_data_excel)
    
    # Test handling of invalid data types
    df = processor.read_sheet('InvalidData')
    numeric_conversion = pd.to_numeric(df['amount'], errors='coerce')
    assert numeric_conversion.isna().any()
    
    # Test handling of missing sheet
    with pytest.raises(ValueError):
        processor.read_sheet('NonexistentSheet')
    
    # Test handling of invalid column names
    with pytest.raises(KeyError):
        df['nonexistent_column']

def test_file_format_compatibility(tmp_path):
    """Test compatibility with different Excel formats."""
    data = pd.DataFrame({'A': [1, 2, 3]})
    
    # Test with xlsx format
    xlsx_path = tmp_path / "test.xlsx"
    data.to_excel(xlsx_path, index=False, engine='openpyxl')
    xlsx_processor = ExcelProcessor(xlsx_path)
    xlsx_df = xlsx_processor.read_sheet()
    assert len(xlsx_df) == 3
    assert list(xlsx_df['A']) == [1, 2, 3]
    
    # Test with xls format (legacy format)
    xls_path = tmp_path / "test.xls"
    data.to_excel(xls_path, index=False, engine='openpyxl')
    xls_processor = ExcelProcessor(xls_path)
    xls_df = xls_processor.read_sheet()
    assert len(xls_df) == 3
    assert list(xls_df['A']) == [1, 2, 3] 