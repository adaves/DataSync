"""
Database validation module for ensuring data integrity and consistency.
This module provides validation functions for database operations including
pre-insert, pre-update, and pre-delete validations.
"""

from typing import Dict, Any, List, Optional, Tuple, Union
import logging
from datetime import datetime
import re
from dataclasses import dataclass
from datasync.database.operations import DatabaseOperations

@dataclass
class ValidationResult:
    """Result of a validation operation."""
    is_valid: bool
    errors: List[str] = None
    
    def __init__(self, is_valid: bool = True, errors: List[str] = None):
        self.is_valid = is_valid
        self.errors = errors or []

class DatabaseValidation:
    """Handles validation of database operations and data integrity."""
    
    def __init__(self, db: Union[DatabaseOperations, Any], logger: Optional[logging.Logger] = None) -> None:
        """
        Initialize the validation class.
        
        Args:
            db: Database connection object or DatabaseOperations instance
            logger: Optional logger instance for validation logging
        """
        self.db = getattr(db, 'db', db) if hasattr(db, 'db') else db
        self.logger = logger or logging.getLogger(__name__)
        
        # Access-specific type mappings
        self.access_types = {
            'TEXT': str,
            'MEMO': str,
            'NUMBER': (int, float),
            'INTEGER': int,
            'LONG': int,
            'SINGLE': float,
            'DOUBLE': float,
            'CURRENCY': float,
            'DATETIME': datetime,
            'BOOLEAN': bool,
            'BINARY': bytes
        }
    
    def validate_record_structure(self, record: Dict[str, Any], 
                                expected_fields: List[str]) -> bool:
        """
        Validate that a record has the expected structure.
        
        Args:
            record: Dictionary representing the record to validate
            expected_fields: List of field names that should be present
            
        Returns:
            True if structure is valid, False otherwise
        """
        return all(field in record for field in expected_fields)
    
    def validate_data_types(self, data: Dict[str, Any], schema: Dict[str, type],
                          validate_types: bool = True) -> bool:
        """
        Validate that data types match the expected schema.
        
        Args:
            data: Dictionary of field names and values to validate
            schema: Dictionary mapping field names to expected types
            validate_types: Whether to validate types (default: True)
            
        Returns:
            True if validation passes, False otherwise
        """
        if not validate_types:
            return True
            
        for field, value in data.items():
            if field not in schema:
                continue
                
            expected_type = schema[field]
            if value is None:
                continue
                
            if not isinstance(value, expected_type):
                return False
        
        return True
    
    def validate_required_fields(self, data: Dict[str, Any], 
                               required_fields: List[str]) -> bool:
        """
        Validate that all required fields are present and not None.
        
        Args:
            data: Dictionary of field names and values to validate
            required_fields: List of field names that are required
            
        Returns:
            True if validation passes, False otherwise
        """
        return all(field in data and data[field] is not None 
                  for field in required_fields)
    
    def validate_field_lengths(self, data: Dict[str, Any], 
                             max_lengths: Dict[str, int]) -> bool:
        """
        Validate that string fields don't exceed maximum lengths.
        
        Args:
            data: Dictionary of field names and values to validate
            max_lengths: Dictionary mapping field names to maximum lengths
            
        Returns:
            True if validation passes, False otherwise
        """
        for field, max_len in max_lengths.items():
            if field in data and isinstance(data[field], str):
                if len(data[field]) > max_len:
                    return False
        return True
    
    def validate_unique_constraints(self, data: Dict[str, Any],
                                  unique_fields: List[str]) -> bool:
        """
        Validate that unique field constraints are satisfied.
        
        Args:
            data: Dictionary of field names and values to validate
            unique_fields: List of field names that should be unique
            
        Returns:
            True if validation passes, False otherwise
        """
        # This is a placeholder - actual implementation would check against database
        return True
    
    def validate_foreign_keys(self, data: Dict[str, Any],
                            foreign_keys: Dict[str, List[Any]]) -> bool:
        """
        Validate that foreign key fields reference valid values.
        
        Args:
            data: Dictionary of field names and values to validate
            foreign_keys: Dictionary mapping field names to lists of valid values
            
        Returns:
            True if validation passes, False otherwise
        """
        for field, valid_values in foreign_keys.items():
            if field in data and data[field] is not None:
                if data[field] not in valid_values:
                    return False
        return True
    
    def validate_date_ranges(self, data: Dict[str, Any],
                           date_ranges: Dict[str, Tuple[datetime, datetime]]) -> bool:
        """
        Validate that date fields fall within specified ranges.
        
        Args:
            data: Dictionary of field names and values to validate
            date_ranges: Dictionary mapping field names to (min_date, max_date) tuples
            
        Returns:
            True if validation passes, False otherwise
        """
        for field, (min_date, max_date) in date_ranges.items():
            if field in data and isinstance(data[field], datetime):
                if data[field] < min_date or data[field] > max_date:
                    return False
        return True
    
    def validate_numeric_ranges(self, data: Dict[str, Any],
                              numeric_ranges: Dict[str, Tuple[float, float]]) -> bool:
        """
        Validate that numeric fields fall within specified ranges.
        
        Args:
            data: Dictionary of field names and values to validate
            numeric_ranges: Dictionary mapping field names to (min_value, max_value) tuples
            
        Returns:
            True if validation passes, False otherwise
        """
        for field, (min_val, max_val) in numeric_ranges.items():
            if field in data and isinstance(data[field], (int, float)):
                if data[field] < min_val or data[field] > max_val:
                    return False
        return True
    
    def validate_string_length(self, data: Dict[str, Any], 
                             max_lengths: Dict[str, int]) -> List[str]:
        """
        Validate that string fields don't exceed maximum lengths.
        
        Args:
            data: Dictionary of field names and values to validate
            max_lengths: Dictionary mapping field names to maximum lengths
            
        Returns:
            List of validation error messages, empty if validation passes
        """
        errors = []
        for field, max_len in max_lengths.items():
            if field in data and isinstance(data[field], str):
                if len(data[field]) > max_len:
                    errors.append(
                        f"Field '{field}' exceeds maximum length of {max_len} characters"
                    )
        
        return errors
    
    def validate_date_range(self, data: Dict[str, Any], 
                          date_fields: Dict[str, tuple]) -> List[str]:
        """
        Validate that date fields fall within specified ranges.
        
        Args:
            data: Dictionary of field names and values to validate
            date_fields: Dictionary mapping field names to (min_date, max_date) tuples
            
        Returns:
            List of validation error messages, empty if validation passes
        """
        errors = []
        for field, (min_date, max_date) in date_fields.items():
            if field in data and isinstance(data[field], datetime):
                if data[field] < min_date or data[field] > max_date:
                    errors.append(
                        f"Field '{field}' date {data[field]} is outside valid range "
                        f"({min_date} to {max_date})"
                    )
        
        return errors
    
    def validate_pattern(self, data: Dict[str, Any], 
                        patterns: Dict[str, str]) -> List[str]:
        """
        Validate that string fields match specified regex patterns.
        
        Args:
            data: Dictionary of field names and values to validate
            patterns: Dictionary mapping field names to regex patterns
            
        Returns:
            List of validation error messages, empty if validation passes
        """
        errors = []
        for field, pattern in patterns.items():
            if field in data and isinstance(data[field], str):
                if not re.match(pattern, data[field]):
                    errors.append(
                        f"Field '{field}' value '{data[field]}' "
                        f"does not match pattern '{pattern}'"
                    )
        
        return errors
    
    def validate_all(self, data: Dict[str, Any], 
                    validation_rules: Dict[str, Any]) -> List[str]:
        """
        Run all specified validations on the data.
        
        Args:
            data: Dictionary of field names and values to validate
            validation_rules: Dictionary containing all validation rules
            
        Returns:
            List of validation error messages, empty if validation passes
        """
        errors = []
        
        # Run each validation if its rules are provided
        if 'data_types' in validation_rules:
            errors.extend(self.validate_data_types(data, validation_rules['data_types']))
        
        if 'required_fields' in validation_rules:
            errors.extend(self.validate_required_fields(data, validation_rules['required_fields']))
        
        if 'string_lengths' in validation_rules:
            errors.extend(self.validate_string_length(data, validation_rules['string_lengths']))
        
        if 'date_ranges' in validation_rules:
            errors.extend(self.validate_date_range(data, validation_rules['date_ranges']))
        
        if 'patterns' in validation_rules:
            errors.extend(self.validate_pattern(data, validation_rules['patterns']))
        
        if 'foreign_keys' in validation_rules:
            errors.extend(self.validate_foreign_keys(data, validation_rules['foreign_keys']))
        
        return errors
    
    def validate_table_structure(self, table_name: str, expected_structure: Dict[str, str]) -> ValidationResult:
        """
        Validate that a table has the expected structure.
        
        Args:
            table_name: Name of the table to validate
            expected_structure: Dictionary mapping column names to their expected types
            
        Returns:
            ValidationResult object containing validation status and any error messages
        """
        errors = []
        
        try:
            # Get actual table structure using Access system tables
            query = """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
            """
            actual_structure = {
                row['COLUMN_NAME']: row['DATA_TYPE']
                for row in self.db.execute_query(query, (table_name,))
            }
            
            # Check for missing columns
            missing_columns = set(actual_structure.keys()) - set(expected_structure.keys())
            if missing_columns:
                errors.append(f"Extra columns in table '{table_name}': {', '.join(missing_columns)}")
            
            # Check for extra columns
            extra_columns = set(expected_structure.keys()) - set(actual_structure.keys())
            if extra_columns:
                errors.append(f"Missing columns in table '{table_name}': {', '.join(extra_columns)}")
            
            # Check column types
            for col_name, expected_type in expected_structure.items():
                if col_name in actual_structure:
                    actual_type = actual_structure[col_name]
                    if not self._types_match(actual_type, expected_type):
                        errors.append(
                            f"Invalid column type for '{col_name}' in table '{table_name}': "
                            f"expected {expected_type}, got {actual_type}"
                        )
        except Exception as e:
            errors.append(f"Error validating table structure: {str(e)}")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)
    
    def validate_data_types(self, table_name: str, record: Dict[str, Any]) -> ValidationResult:
        """
        Validate that the data types in a record match the table schema.
        
        Args:
            table_name: Name of the table to validate against
            record: Dictionary containing the record data
            
        Returns:
            ValidationResult object containing validation status and any error messages
        """
        errors = []
        
        try:
            # Get table schema using Access system tables
            query = """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
            """
            columns = self.db.execute_query(query, (table_name,))
            
            for col in columns:
                col_name = col['COLUMN_NAME']
                col_type = col['DATA_TYPE']
                nullable = col['IS_NULLABLE'] == 'YES'
                
                if col_name not in record:
                    if not nullable:
                        errors.append(f"Missing required column '{col_name}' in record")
                    continue
                    
                value = record[col_name]
                if value is None:
                    if not nullable:
                        errors.append(f"Null value not allowed for column '{col_name}'")
                    continue
                
                # Validate type
                if not self._validate_value_type(value, col_type):
                    errors.append(
                        f"Invalid data type for column '{col_name}': expected {col_type}, "
                        f"got {type(value).__name__}"
                    )
        except Exception as e:
            errors.append(f"Error validating data types: {str(e)}")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)
    
    def validate_foreign_keys(self, table_name: str, record: Dict[str, Any]) -> ValidationResult:
        """
        Validate that foreign key references are valid.
        
        Args:
            table_name: Name of the table to validate
            record: Dictionary containing the record data
            
        Returns:
            ValidationResult object containing validation status and any error messages
        """
        errors = []
        
        try:
            # Get foreign key constraints using Access system tables
            query = """
                SELECT 
                    fk.COLUMN_NAME,
                    pk.TABLE_NAME AS REFERENCED_TABLE,
                    pk.COLUMN_NAME AS REFERENCED_COLUMN
                FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
                    ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
                    ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME
                WHERE fk.TABLE_NAME = ?
            """
            foreign_keys = self.db.execute_query(query, (table_name,))
            
            for fk in foreign_keys:
                col_name = fk['COLUMN_NAME']
                ref_table = fk['REFERENCED_TABLE']
                ref_col = fk['REFERENCED_COLUMN']
                
                if col_name in record and record[col_name] is not None:
                    # Check if referenced value exists
                    query = f"SELECT 1 FROM {ref_table} WHERE {ref_col} = ?"
                    result = self.db.execute_query(query, (record[col_name],))
                    if not result:
                        errors.append(
                            f"Invalid foreign key value in column '{col_name}': "
                            f"no matching record found in table '{ref_table}'"
                        )
        except Exception as e:
            errors.append(f"Error validating foreign keys: {str(e)}")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)
    
    def validate_unique_constraints(self, table_name: str, record: Dict[str, Any]) -> ValidationResult:
        """
        Validate that unique constraints are not violated.
        
        Args:
            table_name: Name of the table to validate
            record: Dictionary containing the record data
            
        Returns:
            ValidationResult object containing validation status and any error messages
        """
        errors = []
        
        try:
            # Get unique constraints using Access system tables
            query = """
                SELECT 
                    tc.CONSTRAINT_NAME,
                    kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                WHERE tc.TABLE_NAME = ?
                AND tc.CONSTRAINT_TYPE = 'UNIQUE'
            """
            constraints = self.db.execute_query(query, (table_name,))
            
            # Group columns by constraint
            constraint_columns = {}
            for row in constraints:
                constraint_name = row['CONSTRAINT_NAME']
                if constraint_name not in constraint_columns:
                    constraint_columns[constraint_name] = []
                constraint_columns[constraint_name].append(row['COLUMN_NAME'])
            
            # Check each unique constraint
            for constraint_name, columns in constraint_columns.items():
                conditions = []
                params = []
                for col in columns:
                    if col in record:
                        conditions.append(f"{col} = ?")
                        params.append(record[col])
                
                if conditions:
                    where_clause = " AND ".join(conditions)
                    query = f"SELECT 1 FROM {table_name} WHERE {where_clause}"
                    result = self.db.execute_query(query, params)
                    if result:
                        errors.append(
                            f"Unique constraint violation for columns: {', '.join(columns)}"
                        )
        except Exception as e:
            errors.append(f"Error validating unique constraints: {str(e)}")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)
    
    def validate_check_constraints(self, table_name: str, record: Dict[str, Any]) -> ValidationResult:
        """
        Validate that check constraints are not violated.
        
        Args:
            table_name: Name of the table to validate
            record: Dictionary containing the record data
            
        Returns:
            ValidationResult object containing validation status and any error messages
        """
        errors = []
        
        try:
            # Get check constraints using Access system tables
            query = """
                SELECT CONSTRAINT_NAME, CHECK_CLAUSE
                FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS
                WHERE TABLE_NAME = ?
            """
            constraints = self.db.execute_query(query, (table_name,))
            
            for constraint in constraints:
                check_clause = constraint['CHECK_CLAUSE']
                
                # Replace column references with actual values
                check_sql = check_clause
                for col_name, value in record.items():
                    check_sql = re.sub(
                        rf"\b{col_name}\b",
                        str(value) if value is not None else "NULL",
                        check_sql
                    )
                
                # Evaluate the check constraint
                try:
                    query = f"SELECT CASE WHEN {check_sql} THEN 1 ELSE 0 END"
                    result = self.db.execute_query(query)
                    if not result or not result[0][0]:
                        errors.append(f"Check constraint violation: {check_clause}")
                except Exception as e:
                    errors.append(f"Error evaluating check constraint '{check_clause}': {str(e)}")
        except Exception as e:
            errors.append(f"Error validating check constraints: {str(e)}")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)
    
    def validate_record(self, table_name: str, record: Dict[str, Any]) -> ValidationResult:
        """
        Perform complete validation of a single record.
        
        Args:
            table_name: Name of the table to validate
            record: Dictionary containing the record data
            
        Returns:
            ValidationResult object containing validation status and any error messages
        """
        all_errors = []
        
        # Validate data types
        result = self.validate_data_types(table_name, record)
        if not result.is_valid:
            all_errors.extend(result.errors)
        
        # Validate foreign keys
        result = self.validate_foreign_keys(table_name, record)
        if not result.is_valid:
            all_errors.extend(result.errors)
        
        # Validate unique constraints
        result = self.validate_unique_constraints(table_name, record)
        if not result.is_valid:
            all_errors.extend(result.errors)
        
        # Validate check constraints
        result = self.validate_check_constraints(table_name, record)
        if not result.is_valid:
            all_errors.extend(result.errors)
        
        return ValidationResult(is_valid=len(all_errors) == 0, errors=all_errors)
    
    def validate_batch_records(self, table_name: str, records: List[Dict[str, Any]]) -> ValidationResult:
        """
        Validate a batch of records.
        
        Args:
            table_name: Name of the table to validate
            records: List of dictionaries containing the record data
            
        Returns:
            ValidationResult object containing validation status and any error messages
        """
        all_errors = []
        
        for i, record in enumerate(records):
            result = self.validate_record(table_name, record)
            if not result.is_valid:
                all_errors.extend([f"Record {i}: {error}" for error in result.errors])
        
        return ValidationResult(is_valid=len(all_errors) == 0, errors=all_errors)
    
    def _validate_value_type(self, value: Any, expected_type: str) -> bool:
        """
        Validate that a value matches the expected Access type.
        
        Args:
            value: The value to validate
            expected_type: The expected Access type
            
        Returns:
            bool: True if the value matches the expected type, False otherwise
        """
        expected_type = expected_type.upper()
        
        if expected_type not in self.access_types:
            return False
            
        expected_types = self.access_types[expected_type]
        if isinstance(expected_types, tuple):
            return isinstance(value, expected_types)
        return isinstance(value, expected_types)
    
    def _types_match(self, actual_type: str, expected_type: str) -> bool:
        """
        Check if two Access types match, accounting for variations in type specifications.
        
        Args:
            actual_type: The actual type from the database
            expected_type: The expected type specification
            
        Returns:
            bool: True if types match, False otherwise
        """
        # Normalize types for comparison
        actual = actual_type.upper()
        expected = expected_type.upper()
        
        # Handle exact matches
        if actual == expected:
            return True
        
        # Handle type variations
        text_types = {'TEXT', 'MEMO', 'VARCHAR', 'CHAR'}
        numeric_types = {'NUMBER', 'INTEGER', 'LONG', 'SINGLE', 'DOUBLE', 'CURRENCY'}
        
        if actual in text_types and expected in text_types:
            return True
        if actual in numeric_types and expected in numeric_types:
            return True
            
        return False 