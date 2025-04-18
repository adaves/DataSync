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
    dt = datetime(2024, 3, 14, 15, 30, 45)
    assert format_timestamp(dt) == "2024-03-14 15:30:45"

def test_safe_get():
    """Test safe dictionary access."""
    d = {'a': 1, 'b': 2}
    assert safe_get(d, 'a') == 1
    assert safe_get(d, 'c') is None
    assert safe_get(d, 'c', 'default') == 'default'

def test_ensure_directory(tmp_path):
    """Test directory creation."""
    test_dir = tmp_path / "test_dir" / "nested"
    ensure_directory(test_dir)
    assert test_dir.exists()
    assert test_dir.is_dir()

def test_chunk_list():
    """Test list chunking."""
    items = list(range(10))
    chunks = chunk_list(items, 3)
    assert len(chunks) == 4
    assert chunks == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

def test_validate_dataframe():
    """Test DataFrame validation."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [1.0, 2.0, 3.0]
    })
    
    # Test required columns
    assert validate_dataframe(df, ['id', 'name'])
    assert not validate_dataframe(df, ['id', 'missing'])
    
    # Test column types
    assert validate_dataframe(df, column_types={
        'id': 'int64',
        'name': 'object',
        'value': 'float64'
    })
    
    assert not validate_dataframe(df, column_types={
        'id': 'float64',  # Wrong type
        'name': 'object'
    }) 