"""
Unit tests for database validation.
This module contains tests for the DatabaseValidation class and its methods.
"""

import pytest
from datetime import datetime
from datasync.database.validation import DatabaseValidation

@pytest.fixture
def db_validation():
    """Create a DatabaseValidation instance."""
    return DatabaseValidation()

class TestDatabaseValidation:
    """Test suite for DatabaseValidation class."""
    
    def test_validate_data_types(self, db_validation):
        """Test data type validation."""
        data = {
            "id": 1,
            "name": "Test",
            "value": 42.5,
            "date": datetime.now(),
            "optional": None
        }
        schema = {
            "id": int,
            "name": str,
            "value": float,
            "date": datetime,
            "optional": str
        }
        
        errors = db_validation.validate_data_types(data, schema)
        assert len(errors) == 0
    
    def test_validate_data_types_invalid(self, db_validation):
        """Test data type validation with invalid types."""
        data = {
            "id": "1",  # Should be int
            "name": 42,  # Should be str
            "value": "42.5",  # Should be float
            "date": "2023-01-01"  # Should be datetime
        }
        schema = {
            "id": int,
            "name": str,
            "value": float,
            "date": datetime
        }
        
        errors = db_validation.validate_data_types(data, schema)
        assert len(errors) == 4
    
    def test_validate_required_fields(self, db_validation):
        """Test required field validation."""
        data = {
            "id": 1,
            "name": "Test",
            "value": 42.5
        }
        required = ["id", "name"]
        
        errors = db_validation.validate_required_fields(data, required)
        assert len(errors) == 0
    
    def test_validate_required_fields_missing(self, db_validation):
        """Test required field validation with missing fields."""
        data = {
            "id": 1,
            "value": 42.5
        }
        required = ["id", "name", "date"]
        
        errors = db_validation.validate_required_fields(data, required)
        assert len(errors) == 2
        assert any("name" in error for error in errors)
        assert any("date" in error for error in errors)
    
    def test_validate_string_length(self, db_validation):
        """Test string length validation."""
        data = {
            "name": "Test",
            "description": "A short description"
        }
        max_lengths = {
            "name": 10,
            "description": 50
        }
        
        errors = db_validation.validate_string_length(data, max_lengths)
        assert len(errors) == 0
    
    def test_validate_string_length_exceeded(self, db_validation):
        """Test string length validation with exceeded lengths."""
        data = {
            "name": "This is a very long name",
            "description": "A" * 51
        }
        max_lengths = {
            "name": 10,
            "description": 50
        }
        
        errors = db_validation.validate_string_length(data, max_lengths)
        assert len(errors) == 2
    
    def test_validate_date_range(self, db_validation):
        """Test date range validation."""
        min_date = datetime(2020, 1, 1)
        max_date = datetime(2023, 12, 31)
        data = {
            "start_date": datetime(2021, 6, 15),
            "end_date": datetime(2023, 6, 15)
        }
        date_ranges = {
            "start_date": (min_date, max_date),
            "end_date": (min_date, max_date)
        }
        
        errors = db_validation.validate_date_range(data, date_ranges)
        assert len(errors) == 0
    
    def test_validate_date_range_invalid(self, db_validation):
        """Test date range validation with invalid dates."""
        min_date = datetime(2020, 1, 1)
        max_date = datetime(2023, 12, 31)
        data = {
            "start_date": datetime(2019, 12, 31),
            "end_date": datetime(2024, 1, 1)
        }
        date_ranges = {
            "start_date": (min_date, max_date),
            "end_date": (min_date, max_date)
        }
        
        errors = db_validation.validate_date_range(data, date_ranges)
        assert len(errors) == 2
    
    def test_validate_pattern(self, db_validation):
        """Test pattern validation."""
        data = {
            "email": "test@example.com",
            "phone": "+1-234-567-8900"
        }
        patterns = {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "phone": r"^\+\d{1,3}-\d{3}-\d{3}-\d{4}$"
        }
        
        errors = db_validation.validate_pattern(data, patterns)
        assert len(errors) == 0
    
    def test_validate_pattern_invalid(self, db_validation):
        """Test pattern validation with invalid patterns."""
        data = {
            "email": "invalid-email",
            "phone": "123-456-7890"
        }
        patterns = {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "phone": r"^\+\d{1,3}-\d{3}-\d{3}-\d{4}$"
        }
        
        errors = db_validation.validate_pattern(data, patterns)
        assert len(errors) == 2
    
    def test_validate_foreign_key(self, db_validation):
        """Test foreign key validation."""
        data = {
            "category_id": 1,
            "status_id": 2
        }
        foreign_keys = {
            "category_id": [1, 2, 3],
            "status_id": [1, 2]
        }
        
        errors = db_validation.validate_foreign_key(data, foreign_keys)
        assert len(errors) == 0
    
    def test_validate_foreign_key_invalid(self, db_validation):
        """Test foreign key validation with invalid references."""
        data = {
            "category_id": 4,
            "status_id": 3
        }
        foreign_keys = {
            "category_id": [1, 2, 3],
            "status_id": [1, 2]
        }
        
        errors = db_validation.validate_foreign_key(data, foreign_keys)
        assert len(errors) == 2
    
    def test_validate_all(self, db_validation):
        """Test comprehensive validation."""
        data = {
            "id": 1,
            "name": "Test",
            "email": "test@example.com",
            "date": datetime(2021, 6, 15),
            "category_id": 1
        }
        validation_rules = {
            "data_types": {
                "id": int,
                "name": str,
                "email": str,
                "date": datetime,
                "category_id": int
            },
            "required_fields": ["id", "name", "email"],
            "string_lengths": {
                "name": 10,
                "email": 50
            },
            "patterns": {
                "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            },
            "foreign_keys": {
                "category_id": [1, 2, 3]
            }
        }
        
        errors = db_validation.validate_all(data, validation_rules)
        assert len(errors) == 0 