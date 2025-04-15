"""
Database validation module for ensuring data integrity and consistency.
This module provides validation functions for database operations including
pre-insert, pre-update, and pre-delete validations.
"""

from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import re

class DatabaseValidation:
    """Handles validation of database operations and data integrity."""
    
    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """
        Initialize the validation class.
        
        Args:
            logger: Optional logger instance for validation logging
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def validate_data_types(self, data: Dict[str, Any], schema: Dict[str, type]) -> List[str]:
        """
        Validate that data types match the expected schema.
        
        Args:
            data: Dictionary of field names and values to validate
            schema: Dictionary mapping field names to expected types
            
        Returns:
            List of validation error messages, empty if validation passes
        """
        errors = []
        for field, value in data.items():
            if field not in schema:
                continue
                
            expected_type = schema[field]
            if value is None:
                continue
                
            if not isinstance(value, expected_type):
                errors.append(
                    f"Field '{field}' has type {type(value).__name__}, "
                    f"expected {expected_type.__name__}"
                )
        
        return errors
    
    def validate_required_fields(self, data: Dict[str, Any], required_fields: List[str]) -> List[str]:
        """
        Validate that all required fields are present and not None.
        
        Args:
            data: Dictionary of field names and values to validate
            required_fields: List of field names that are required
            
        Returns:
            List of validation error messages, empty if validation passes
        """
        errors = []
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"Required field '{field}' is missing or None")
        
        return errors
    
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
    
    def validate_foreign_key(self, data: Dict[str, Any], 
                           foreign_keys: Dict[str, List[Any]]) -> List[str]:
        """
        Validate that foreign key fields reference valid values.
        
        Args:
            data: Dictionary of field names and values to validate
            foreign_keys: Dictionary mapping field names to lists of valid values
            
        Returns:
            List of validation error messages, empty if validation passes
        """
        errors = []
        for field, valid_values in foreign_keys.items():
            if field in data and data[field] is not None:
                if data[field] not in valid_values:
                    errors.append(
                        f"Field '{field}' value '{data[field]}' is not a valid reference"
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
            errors.extend(self.validate_foreign_key(data, validation_rules['foreign_keys']))
        
        return errors 