"""
Unit tests for database validation functionality.
"""

import pytest
from datetime import datetime
from datasync.database.validation import DatabaseValidation
from datasync.utils.error_handling import ValidationError
from tests.fixtures.database import db_validation, setup_test_table, sample_records

class TestDatabaseValidation:
    """Test suite for DatabaseValidation class."""
    
    def test_validate_table_structure(self, db_validation, setup_test_table):
        """Test table structure validation."""
        table_name, _ = setup_test_table
        
        # Valid table structure
        result = db_validation.validate_table_structure(table_name, {
            'id': 'INTEGER',
            'name': 'TEXT(50)',
            'value': 'DOUBLE',
            'created_at': 'DATETIME'
        })
        assert result.is_valid
        
        # Invalid column type
        result = db_validation.validate_table_structure(table_name, {
            'id': 'INTEGER',
            'name': 'INTEGER',  # Should be TEXT
            'value': 'DOUBLE',
            'created_at': 'DATETIME'
        })
        assert not result.is_valid
        assert any('Invalid column type' in str(e) for e in result.errors)
        
        # Missing column
        result = db_validation.validate_table_structure(table_name, {
            'id': 'INTEGER',
            'name': 'TEXT(50)',
            'value': 'DOUBLE'
            # Missing created_at
        })
        assert not result.is_valid
        assert any('Missing column' in str(e) for e in result.errors)
        
        # Extra column
        result = db_validation.validate_table_structure(table_name, {
            'id': 'INTEGER',
            'name': 'TEXT(50)',
            'value': 'DOUBLE',
            'created_at': 'DATETIME',
            'extra_column': 'TEXT(50)'  # Extra column
        })
        assert not result.is_valid
        assert any('Extra column' in str(e) for e in result.errors)
    
    def test_validate_data_types(self, db_validation, setup_test_table):
        """Test data type validation."""
        table_name, _ = setup_test_table
        
        # Valid data types
        result = db_validation.validate_data_types(table_name, {
            'id': 1,
            'name': 'Test',
            'value': 10.5,
            'created_at': datetime.now()
        })
        assert result.is_valid
        
        # Invalid data type
        result = db_validation.validate_data_types(table_name, {
            'id': 'invalid',  # Should be integer
            'name': 'Test',
            'value': 10.5,
            'created_at': datetime.now()
        })
        assert not result.is_valid
        assert any('Invalid data type' in str(e) for e in result.errors)
        
        # Null value in non-nullable column
        result = db_validation.validate_data_types(table_name, {
            'id': 1,
            'name': None,  # Should not be null
            'value': 10.5,
            'created_at': datetime.now()
        })
        assert not result.is_valid
        assert any('Null value' in str(e) for e in result.errors)
    
    def test_validate_foreign_keys(self, db_validation):
        """Test foreign key validation."""
        # Create related tables
        db_validation.db.execute_query("""
            CREATE TABLE parent_table (
                id INTEGER PRIMARY KEY,
                name TEXT(50)
            )
        """)
        
        db_validation.db.execute_query("""
            CREATE TABLE child_table (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                value TEXT(50),
                FOREIGN KEY (parent_id) REFERENCES parent_table(id)
            )
        """)
        
        # Insert parent record
        db_validation.db.execute_query(
            "INSERT INTO parent_table (id, name) VALUES (?, ?)",
            (1, 'Parent 1')
        )
        
        # Valid foreign key
        result = db_validation.validate_foreign_keys('child_table', {
            'id': 1,
            'parent_id': 1,  # Valid parent ID
            'value': 'Test'
        })
        assert result.is_valid
        
        # Invalid foreign key
        result = db_validation.validate_foreign_keys('child_table', {
            'id': 2,
            'parent_id': 999,  # Non-existent parent ID
            'value': 'Test'
        })
        assert not result.is_valid
        assert any('Invalid foreign key' in str(e) for e in result.errors)
        
        # Cleanup
        db_validation.db.execute_query("DROP TABLE child_table")
        db_validation.db.execute_query("DROP TABLE parent_table")
    
    def test_validate_unique_constraints(self, db_validation, setup_test_table):
        """Test unique constraint validation."""
        table_name, _ = setup_test_table
        
        # Add unique constraint
        db_validation.db.execute_query(
            f"CREATE UNIQUE INDEX idx_name ON {table_name} (name)"
        )
        
        # Valid unique value
        result = db_validation.validate_unique_constraints(table_name, {
            'id': 4,
            'name': 'Unique Name',  # Unique value
            'value': 40.5,
            'created_at': datetime.now()
        })
        assert result.is_valid
        
        # Duplicate unique value
        result = db_validation.validate_unique_constraints(table_name, {
            'id': 5,
            'name': 'Test 1',  # Duplicate value
            'value': 50.5,
            'created_at': datetime.now()
        })
        assert not result.is_valid
        assert any('Duplicate value' in str(e) for e in result.errors)
    
    def test_validate_check_constraints(self, db_validation, setup_test_table):
        """Test check constraint validation."""
        table_name, _ = setup_test_table
        
        # Add check constraint
        db_validation.db.execute_query(
            f"ALTER TABLE {table_name} ADD CONSTRAINT chk_value CHECK (value > 0)"
        )
        
        # Valid check constraint
        result = db_validation.validate_check_constraints(table_name, {
            'id': 6,
            'name': 'Test 6',
            'value': 10.5,  # Valid value
            'created_at': datetime.now()
        })
        assert result.is_valid
        
        # Invalid check constraint
        result = db_validation.validate_check_constraints(table_name, {
            'id': 7,
            'name': 'Test 7',
            'value': -5.0,  # Invalid value
            'created_at': datetime.now()
        })
        assert not result.is_valid
        assert any('Check constraint violation' in str(e) for e in result.errors)
    
    def test_validate_record(self, db_validation, setup_test_table):
        """Test complete record validation."""
        table_name, _ = setup_test_table
        
        # Valid record
        result = db_validation.validate_record(table_name, {
            'id': 8,
            'name': 'Test 8',
            'value': 80.5,
            'created_at': datetime.now()
        })
        assert result.is_valid
        
        # Invalid record (multiple issues)
        result = db_validation.validate_record(table_name, {
            'id': 'invalid',  # Invalid type
            'name': None,  # Null value
            'value': -5.0,  # Check constraint violation
            'created_at': 'invalid'  # Invalid type
        })
        assert not result.is_valid
        assert len(result.errors) >= 3
    
    def test_validate_batch_records(self, db_validation, setup_test_table):
        """Test batch record validation."""
        table_name, _ = setup_test_table
        
        # Valid batch
        records = [
            {
                'id': 9,
                'name': 'Test 9',
                'value': 90.5,
                'created_at': datetime.now()
            },
            {
                'id': 10,
                'name': 'Test 10',
                'value': 100.5,
                'created_at': datetime.now()
            }
        ]
        result = db_validation.validate_batch_records(table_name, records)
        assert result.is_valid
        
        # Invalid batch
        records = [
            {
                'id': 11,
                'name': None,  # Invalid
                'value': 110.5,
                'created_at': datetime.now()
            },
            {
                'id': 12,
                'name': 'Test 12',
                'value': -5.0,  # Invalid
                'created_at': datetime.now()
            }
        ]
        result = db_validation.validate_batch_records(table_name, records)
        assert not result.is_valid
        assert len(result.errors) == 2 