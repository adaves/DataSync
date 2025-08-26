"""
Unit tests for helper functions.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from datasync.utils.helpers import (
    format_timestamp,
    safe_get,
    ensure_directory,
    chunk_list,
    validate_dataframe
)

def test_format_timestamp():
    """Test timestamp formatting."""
    # Test standard case
    dt = datetime(2024, 3, 14, 15, 30, 45)
    assert format_timestamp(dt) == "2024-03-14 15:30:45"
    
    # Test edge cases
    dt_min = datetime(1, 1, 1, 0, 0, 0)
    assert format_timestamp(dt_min) == "0001-01-01 00:00:00"
    
    dt_max = datetime(9999, 12, 31, 23, 59, 59)
    assert format_timestamp(dt_max) == "9999-12-31 23:59:59"

def test_safe_get():
    """Test safe dictionary access."""
    d = {'a': 1, 'b': 2}
    
    # Test existing keys
    assert safe_get(d, 'a') == 1
    assert safe_get(d, 'b') == 2
    
    # Test non-existent keys
    assert safe_get(d, 'c') is None
    assert safe_get(d, 'c', 'default') == 'default'
    
    # Test with None as default
    assert safe_get(d, 'c', None) is None
    
    # Test with empty dictionary
    assert safe_get({}, 'a') is None
    assert safe_get({}, 'a', 'default') == 'default'
    
    # Test with None dictionary
    with pytest.raises(AttributeError):
        safe_get(None, 'a')

def test_ensure_directory(tmp_path):
    """Test directory creation."""
    # Test creating new directory
    test_dir = tmp_path / "test_dir" / "nested"
    ensure_directory(test_dir)
    assert test_dir.exists()
    assert test_dir.is_dir()
    
    # Test creating existing directory
    ensure_directory(test_dir)  # Should not raise error
    assert test_dir.exists()
    
    # Test with Path object
    path_obj = Path(tmp_path) / "path_obj"
    ensure_directory(path_obj)
    assert path_obj.exists()
    
    # Test with string path
    str_path = str(tmp_path / "str_path")
    ensure_directory(Path(str_path))
    assert Path(str_path).exists()

def test_chunk_list():
    """Test list chunking."""
    # Test standard case
    items = list(range(10))
    chunks = chunk_list(items, 3)
    assert len(chunks) == 4
    assert chunks == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    
    # Test empty list
    assert chunk_list([], 3) == []
    
    # Test chunk size larger than list
    assert chunk_list([1, 2, 3], 5) == [[1, 2, 3]]
    
    # Test chunk size of 1
    assert chunk_list([1, 2, 3], 1) == [[1], [2], [3]]
    
    # Test chunk size equal to list length
    assert chunk_list([1, 2, 3], 3) == [[1, 2, 3]]
    
    # Test with non-list iterable
    assert chunk_list(range(5), 2) == [[0, 1], [2, 3], [4]]

def test_validate_dataframe():
    """Test DataFrame validation."""
    # Create test DataFrame
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [1.0, 2.0, 3.0],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
        'nullable': [1, None, 3]
    })
    
    # Test required columns
    assert validate_dataframe(df, ['id', 'name'])
    assert not validate_dataframe(df, ['id', 'missing'])
    
    # Test column types with string specifications
    assert validate_dataframe(df, column_types={
        'id': 'int64',
        'name': 'object',
        'value': 'float64',
        'date': 'datetime64[ns]'
    })
    
    # Test column types with Python types
    assert validate_dataframe(df, column_types={
        'id': int,
        'name': str,
        'value': float,
        'date': pd.Timestamp
    })
    
    # Test invalid type specifications
    assert not validate_dataframe(df, column_types={
        'id': 'float64',  # Wrong type
        'name': 'object'
    })
    
    # Test with empty DataFrame
    empty_df = pd.DataFrame()
    assert validate_dataframe(empty_df, [])
    assert not validate_dataframe(empty_df, ['id'])
    
    # Test with None DataFrame
    assert not validate_dataframe(None, ['id'])
    
    # Test with mixed types in column
    mixed_df = pd.DataFrame({
        'mixed': [1, '2', 3.0]
    })
    assert not validate_dataframe(mixed_df, column_types={'mixed': int})
    
    # Test with nullable columns
    assert validate_dataframe(df, column_types={'nullable': 'float64'})  # None is considered float64
    
    # Test with custom dtypes
    custom_df = pd.DataFrame({
        'category': pd.Categorical(['A', 'B', 'C'])
    })
    assert validate_dataframe(custom_df, column_types={'category': 'category'}) 