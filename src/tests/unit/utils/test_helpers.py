import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from src.datasync.utils.helpers import (
    validate_dataframe,
    clean_column_names,
    convert_data_types,
    handle_missing_values,
    generate_unique_id
)

class TestHelpers:
    """Test cases for helper functions."""
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample dataframe for testing."""
        return pd.DataFrame({
            'ID': [1, 2, 3],
            'Name': ['John', 'Jane', 'Bob'],
            'Age': [25, 30, None],
            'Salary': ['1000', '2000', '3000'],
            'Date': ['2023-01-01', '2023-01-02', '2023-01-03']
        })
    
    def test_validate_dataframe(self, sample_dataframe):
        """Test dataframe validation."""
        # Test valid dataframe
        assert validate_dataframe(sample_dataframe) is True
        
        # Test empty dataframe
        empty_df = pd.DataFrame()
        assert validate_dataframe(empty_df) is False
        
        # Test dataframe with no columns
        no_cols_df = pd.DataFrame([])
        assert validate_dataframe(no_cols_df) is False
    
    def test_clean_column_names(self, sample_dataframe):
        """Test column name cleaning."""
        df = sample_dataframe.copy()
        df.columns = ['ID ', ' Name ', 'Age ', ' Salary ', ' Date ']
        
        cleaned_df = clean_column_names(df)
        assert list(cleaned_df.columns) == ['id', 'name', 'age', 'salary', 'date']
    
    def test_convert_data_types(self, sample_dataframe):
        """Test data type conversion."""
        df = sample_dataframe.copy()
        type_mapping = {
            'ID': 'int',
            'Age': 'float',
            'Salary': 'float',
            'Date': 'datetime'
        }
        
        converted_df = convert_data_types(df, type_mapping)
        assert converted_df['ID'].dtype == 'int64'
        assert converted_df['Age'].dtype == 'float64'
        assert converted_df['Salary'].dtype == 'float64'
        assert pd.api.types.is_datetime64_any_dtype(converted_df['Date'])
    
    def test_handle_missing_values(self, sample_dataframe):
        """Test missing value handling."""
        df = sample_dataframe.copy()
        
        # Test fill with mean
        filled_df = handle_missing_values(df, strategy='mean', columns=['Age'])
        assert not filled_df['Age'].isna().any()
        
        # Test fill with median
        filled_df = handle_missing_values(df, strategy='median', columns=['Age'])
        assert not filled_df['Age'].isna().any()
        
        # Test fill with mode
        filled_df = handle_missing_values(df, strategy='mode', columns=['Age'])
        assert not filled_df['Age'].isna().any()
        
        # Test fill with constant
        filled_df = handle_missing_values(df, strategy='constant', value=0, columns=['Age'])
        assert not filled_df['Age'].isna().any()
        assert filled_df['Age'].iloc[2] == 0
    
    def test_generate_unique_id(self):
        """Test unique ID generation."""
        id1 = generate_unique_id()
        id2 = generate_unique_id()
        
        assert isinstance(id1, str)
        assert len(id1) == 32  # MD5 hash length
        assert id1 != id2  # IDs should be unique 