"""
Unit tests for Excel validation functionality.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
from datasync.processing.validation import ExcelValidator, ValidationError, ValidationResult
from datasync.processing.excel_processor import ExcelProcessor

@pytest.fixture
def validator():
    """Create an ExcelValidator instance."""
    return ExcelValidator()

@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'int_col': [1, 2, '3', '4x', None],
        'float_col': [1.0, '2.5', 'invalid', 4, None],
        'str_col': ['a', 'b', 1, None, ''],
        'date_col': ['2024-01-01', 'invalid', None, datetime.now(), '2024-13-01'],
        'bool_col': [True, 'yes', 'invalid', 0, None]
    })

def test_validation_result():
    """Test ValidationResult functionality."""
    result = ValidationResult()
    assert result.is_valid
    assert not result.errors
    assert not result.warnings
    
    # Test adding errors
    error = ValidationError(
        column="test",
        row_index=1,
        value="invalid",
        message="Test error",
        error_type="TestError"
    )
    result.add_error(error)
    assert not result.is_valid
    assert len(result.errors) == 1
    
    # Test adding warnings
    warning = ValidationError(
        column="test",
        row_index=1,
        value="suspicious",
        message="Test warning",
        error_type="TestWarning"
    )
    result.add_warning(warning)
    assert len(result.warnings) == 1
    
    # Test string representation
    result_str = str(result)
    assert "Validation failed" in result_str
    assert "Test error" in result_str
    assert "Test warning" in result_str

def test_sheet_structure_validation(validator):
    """Test sheet structure validation."""
    mock_processor = Mock(spec=ExcelProcessor)
    mock_processor.get_sheet_columns.return_value = ['col1', 'col2']
    mock_processor.is_column_empty.return_value = False
    
    # Test with matching columns
    result = validator.validate_sheet_structure(
        mock_processor,
        'Sheet1',
        ['col1', 'col2']
    )
    assert result.is_valid
    
    # Test with missing columns
    result = validator.validate_sheet_structure(
        mock_processor,
        'Sheet1',
        ['col1', 'col2', 'col3']
    )
    assert not result.is_valid
    assert any('Missing required columns' in str(error) for error in result.errors)
    
    # Test with empty columns
    mock_processor.is_column_empty.return_value = True
    result = validator.validate_sheet_structure(
        mock_processor,
        'Sheet1',
        ['col1', 'col2']
    )
    assert result.is_valid  # Empty columns are warnings, not errors
    assert len(result.warnings) == 2
    
    # Test error handling
    mock_processor.get_sheet_columns.side_effect = Exception("Test error")
    result = validator.validate_sheet_structure(
        mock_processor,
        'Sheet1',
        ['col1']
    )
    assert not result.is_valid
    assert any('Error validating sheet structure' in str(error) for error in result.errors)

def test_data_type_validation(validator, sample_df):
    """Test data type validation."""
    # Test integer validation
    result = validator.validate_data_types(sample_df, {'int_col': 'int'})
    assert not result.is_valid
    assert len([e for e in result.errors if e.row_index == 4]) == 1  # '4x' is invalid
    
    # Test float validation
    result = validator.validate_data_types(sample_df, {'float_col': 'float'})
    assert not result.is_valid
    assert len([e for e in result.errors if e.row_index == 3]) == 1  # 'invalid' is invalid
    
    # Test string validation
    result = validator.validate_data_types(sample_df, {'str_col': 'str'})
    assert not result.is_valid  # Integer 1 is not a string
    
    # Test date validation
    result = validator.validate_data_types(sample_df, {'date_col': 'date'})
    assert not result.is_valid
    assert len([e for e in result.errors if 'invalid' in str(e.value)]) == 1
    
    # Test boolean validation
    result = validator.validate_data_types(sample_df, {'bool_col': 'bool'})
    assert not result.is_valid
    assert len([e for e in result.errors if e.value == 'invalid']) == 1
    
    # Test missing column
    result = validator.validate_data_types(sample_df, {'nonexistent': 'str'})
    assert not result.is_valid
    assert any('Column not found' in str(error) for error in result.errors)
    
    # Test unknown type
    result = validator.validate_data_types(sample_df, {'str_col': 'unknown_type'})
    assert not result.is_valid
    assert any('Unknown type validator' in str(error) for error in result.errors)

def test_custom_validation_rules(validator, sample_df):
    """Test custom validation rules."""
    def validate_positive(value):
        try:
            return float(value) > 0 if pd.notna(value) else True
        except (ValueError, TypeError):
            return False

    def validate_length(value):
        try:
            return len(str(value)) <= 3 if pd.notna(value) else True
        except (ValueError, TypeError):
            return False

    rules = {
        'float_col': validate_positive,
        'str_col': validate_length
    }

    # Create a clean sample for initial test
    clean_df = pd.DataFrame({
        'float_col': [1.0, 2.5, 4.0],
        'str_col': ['a', 'b', 'xyz']
    })
    
    result = validator.validate_custom_rules(clean_df, rules)
    assert result.is_valid  # All valid values
    
    # Test with invalid values
    invalid_df = pd.DataFrame({
        'float_col': [-1.0, 'invalid', None],
        'str_col': ['toolong', 'ok', None]
    })
    
    result = validator.validate_custom_rules(invalid_df, rules)
    assert not result.is_valid
    assert len(result.errors) == 3  # -1.0, 'invalid', and 'toolong' should fail
    
    # Verify specific error cases
    error_values = [error.value for error in result.errors]
    assert -1.0 in error_values  # Negative number
    assert 'invalid' in error_values  # Non-numeric value
    assert 'toolong' in error_values  # String too long
    
    # Test with missing column
    rules['nonexistent'] = validate_positive
    result = validator.validate_custom_rules(invalid_df, rules)
    assert not result.is_valid
    assert any('Column not found' in str(error) for error in result.errors)
    
    # Test with failing rule
    def failing_rule(value):
        raise ValueError("Rule failed")
    
    rules = {'float_col': failing_rule}
    result = validator.validate_custom_rules(invalid_df, rules)
    assert not result.is_valid
    assert any('Rule failed' in str(error) for error in result.errors)

def test_custom_validator_registration(validator):
    """Test custom validator registration."""
    def custom_validator(value):
        return isinstance(value, complex)
    
    validator.register_custom_validator('complex', custom_validator)
    assert 'complex' in validator.type_validators
    
    # Test using custom validator
    df = pd.DataFrame({'complex_col': [1+2j, 'invalid', 3+4j]})
    result = validator.validate_data_types(df, {'complex_col': 'complex'})
    assert not result.is_valid
    assert len([e for e in result.errors if e.value == 'invalid']) == 1

def test_validation_performance(validator):
    """Test validation performance with large dataset."""
    # Create a large DataFrame
    large_df = pd.DataFrame({
        'int_col': range(10000),
        'float_col': [float(i) for i in range(10000)],
        'str_col': [str(i) for i in range(10000)]
    })
    
    import time
    start_time = time.time()
    
    result = validator.validate_data_types(
        large_df,
        {'int_col': 'int', 'float_col': 'float', 'str_col': 'str'}
    )
    
    end_time = time.time()
    assert (end_time - start_time) < 1.0  # Should complete in under 1 second
    assert result.is_valid

def test_type_validator_edge_cases(validator):
    """Test type validators with edge cases."""
    # Test integer validation
    assert validator._validate_int("0")
    assert validator._validate_int(-123)
    assert not validator._validate_int("1.23")
    assert not validator._validate_int("abc")
    
    # Test float validation
    assert validator._validate_float("1.23")
    assert validator._validate_float(-1.23)
    assert validator._validate_float("0")
    assert not validator._validate_float("abc")
    
    # Test string validation
    assert validator._validate_str("")
    assert validator._validate_str("abc")
    assert not validator._validate_str(123)
    
    # Test date validation
    assert validator._validate_date("2024-01-01")
    assert validator._validate_date(datetime.now())
    assert not validator._validate_date("invalid")
    assert not validator._validate_date("2024-13-01")
    
    # Test boolean validation
    assert validator._validate_bool(True)
    assert validator._validate_bool("yes")
    assert validator._validate_bool("true")
    assert validator._validate_bool(1)
    assert not validator._validate_bool("invalid")
    assert not validator._validate_bool(2) 