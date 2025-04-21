"""
Unit tests for database validation.
This module contains tests for the DatabaseValidation class and its methods.
"""

import pytest
from datetime import datetime
from datasync.database.validation import DatabaseValidation
from tests.fixtures.database import db_validation

class TestDatabaseValidation:
    """Test suite for DatabaseValidation class."""
    
    def test_validate_record_structure(self, db_validation):
        """Test record structure validation."""
        # Valid record
        record = {
            "id": 1,
            "name": "Test",
            "value": 42.5,
            "created_date": datetime.now()
        }
        assert db_validation.validate_record_structure(record) is True
        
        # Invalid record (missing required field)
        invalid_record = {
            "id": 1,
            "value": 42.5
        }
        assert db_validation.validate_record_structure(invalid_record) is False
    
    def test_validate_data_types(self, db_validation):
        """Test data type validation."""
        # Valid data types
        record = {
            "id": 1,
            "name": "Test",
            "value": 42.5,
            "is_active": True,
            "created_date": datetime.now()
        }
        assert db_validation.validate_data_types(record) is True
        
        # Invalid data types
        invalid_record = {
            "id": "not_an_integer",
            "name": 123,  # Should be string
            "value": "not_a_float",
            "is_active": "not_a_boolean",
            "created_date": "not_a_date"
        }
        assert db_validation.validate_data_types(invalid_record) is False
    
    def test_validate_required_fields(self, db_validation):
        """Test required field validation."""
        # Valid record with all required fields
        record = {
            "id": 1,
            "name": "Test",
            "value": 42.5
        }
        required_fields = ["id", "name", "value"]
        assert db_validation.validate_required_fields(record, required_fields) is True
        
        # Invalid record (missing required field)
        invalid_record = {
            "id": 1,
            "name": "Test"
        }
        assert db_validation.validate_required_fields(invalid_record, required_fields) is False
    
    def test_validate_field_lengths(self, db_validation):
        """Test field length validation."""
        # Valid field lengths
        record = {
            "name": "Test",  # Length 4
            "description": "A test description"  # Length 18
        }
        field_lengths = {
            "name": 50,
            "description": 100
        }
        assert db_validation.validate_field_lengths(record, field_lengths) is True
        
        # Invalid field lengths
        invalid_record = {
            "name": "A" * 51,  # Exceeds length
            "description": "A" * 101  # Exceeds length
        }
        assert db_validation.validate_field_lengths(invalid_record, field_lengths) is False
    
    def test_validate_unique_constraints(self, db_validation):
        """Test unique constraint validation."""
        # Valid unique values
        record = {
            "id": 1,
            "email": "test@example.com"
        }
        unique_fields = ["id", "email"]
        existing_records = [
            {"id": 2, "email": "other@example.com"},
            {"id": 3, "email": "another@example.com"}
        ]
        assert db_validation.validate_unique_constraints(record, unique_fields, existing_records) is True
        
        # Invalid unique values
        invalid_record = {
            "id": 2,  # Duplicate ID
            "email": "test@example.com"
        }
        assert db_validation.validate_unique_constraints(invalid_record, unique_fields, existing_records) is False
    
    def test_validate_foreign_keys(self, db_validation):
        """Test foreign key validation."""
        # Valid foreign key
        record = {
            "id": 1,
            "parent_id": 100
        }
        foreign_keys = {
            "parent_id": {
                "table": "parents",
                "column": "id",
                "existing_ids": [100, 200, 300]
            }
        }
        assert db_validation.validate_foreign_keys(record, foreign_keys) is True
        
        # Invalid foreign key
        invalid_record = {
            "id": 1,
            "parent_id": 999  # Non-existent parent ID
        }
        assert db_validation.validate_foreign_keys(invalid_record, foreign_keys) is False
    
    def test_validate_date_ranges(self, db_validation):
        """Test date range validation."""
        # Valid date range
        record = {
            "start_date": datetime(2023, 1, 1),
            "end_date": datetime(2023, 12, 31)
        }
        date_ranges = {
            "start_date": {
                "min": datetime(2023, 1, 1),
                "max": datetime(2023, 12, 31)
            },
            "end_date": {
                "min": datetime(2023, 1, 1),
                "max": datetime(2023, 12, 31)
            }
        }
        assert db_validation.validate_date_ranges(record, date_ranges) is True
        
        # Invalid date range
        invalid_record = {
            "start_date": datetime(2022, 12, 31),  # Before min
            "end_date": datetime(2024, 1, 1)  # After max
        }
        assert db_validation.validate_date_ranges(invalid_record, date_ranges) is False
    
    def test_validate_numeric_ranges(self, db_validation):
        """Test numeric range validation."""
        # Valid numeric range
        record = {
            "age": 25,
            "score": 85.5
        }
        numeric_ranges = {
            "age": {
                "min": 0,
                "max": 120
            },
            "score": {
                "min": 0.0,
                "max": 100.0
            }
        }
        assert db_validation.validate_numeric_ranges(record, numeric_ranges) is True
        
        # Invalid numeric range
        invalid_record = {
            "age": -1,  # Below min
            "score": 101.0  # Above max
        }
        assert db_validation.validate_numeric_ranges(invalid_record, numeric_ranges) is False 