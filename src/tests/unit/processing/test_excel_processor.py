"""
Unit tests for Excel processing functionality.
"""

import pytest
import pandas as pd
import os
from pathlib import Path
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
    with pd.ExcelWriter(file_path) as writer:
        df1.to_excel(writer, sheet_name='Sheet1', index=False)
        df2.to_excel(writer, sheet_name='Sheet2', index=False)
    
    return file_path

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

def test_get_sheet_names(sample_excel_file):
    """Test getting sheet names."""
    processor = ExcelProcessor(sample_excel_file)
    sheet_names = processor.get_sheet_names()
    assert isinstance(sheet_names, list)
    assert len(sheet_names) == 2
    assert 'Sheet1' in sheet_names
    assert 'Sheet2' in sheet_names

def test_validate_sheet_structure(sample_excel_file):
    """Test sheet structure validation."""
    processor = ExcelProcessor(sample_excel_file)
    
    # Test with valid columns
    assert processor.validate_sheet_structure('Sheet1', ['id', 'name', 'value'])
    
    # Test with missing columns
    assert not processor.validate_sheet_structure('Sheet1', ['id', 'name', 'value', 'extra']) 