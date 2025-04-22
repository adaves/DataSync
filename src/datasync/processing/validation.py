"""
Excel validation module.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Tuple, Any, Callable
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from datasync.processing.excel_processor import ExcelProcessor

@dataclass
class ValidationError:
    """Represents a validation error."""
    column: str
    row_index: int
    value: Any
    message: str
    error_type: str

class ValidationResult:
    """Holds validation results."""
    
    def __init__(self):
        self.errors: List[ValidationError] = []
        self.warnings: List[ValidationError] = []
        self._is_valid = True
    
    @property
    def is_valid(self) -> bool:
        """Return whether validation passed."""
        return self._is_valid and not self.errors
    
    def add_error(self, error: ValidationError) -> None:
        """Add an error to the result."""
        self.errors.append(error)
        self._is_valid = False
    
    def add_warning(self, warning: ValidationError) -> None:
        """Add a warning to the result."""
        self.warnings.append(warning)
    
    def __str__(self) -> str:
        """Return string representation of validation results."""
        result = []
        if not self.is_valid:
            result.append("Validation failed with the following errors:")
            for error in self.errors:
                result.append(f"  - {error.error_type} in column '{error.column}' at row {error.row_index}: {error.message}")
        if self.warnings:
            result.append("\nWarnings:")
            for warning in self.warnings:
                result.append(f"  - {warning.error_type} in column '{warning.column}' at row {warning.row_index}: {warning.message}")
        return "\n".join(result) if result else "Validation passed successfully"

class ExcelValidator:
    """Handles Excel file validation."""
    
    def __init__(self):
        """Initialize validator with default settings."""
        self.type_validators = {
            'int': self._validate_int,
            'float': self._validate_float,
            'str': self._validate_str,
            'date': self._validate_date,
            'bool': self._validate_bool
        }
        self.custom_validators: Dict[str, Callable] = {}
    
    def validate_sheet_structure(self, 
                               processor: ExcelProcessor,
                               sheet_name: str,
                               required_columns: List[str]) -> ValidationResult:
        """
        Validate sheet structure against required columns.
        
        Args:
            processor: ExcelProcessor instance
            sheet_name: Name of the sheet to validate
            required_columns: List of required column names
            
        Returns:
            ValidationResult containing any errors found
        """
        result = ValidationResult()
        try:
            sheet_columns = processor.get_sheet_columns(sheet_name)
            missing_columns = [col for col in required_columns if col not in sheet_columns]
            
            if missing_columns:
                result.add_error(ValidationError(
                    column="",
                    row_index=0,
                    value=missing_columns,
                    message=f"Missing required columns: {', '.join(missing_columns)}",
                    error_type="StructureError"
                ))
            
            # Check for empty columns
            for col in sheet_columns:
                if processor.is_column_empty(sheet_name, col):
                    result.add_warning(ValidationError(
                        column=col,
                        row_index=0,
                        value=None,
                        message=f"Column '{col}' is empty",
                        error_type="EmptyColumn"
                    ))
            
        except Exception as e:
            result.add_error(ValidationError(
                column="",
                row_index=0,
                value=None,
                message=f"Error validating sheet structure: {str(e)}",
                error_type="ValidationError"
            ))
        
        return result
    
    def validate_data_types(self,
                          df: pd.DataFrame,
                          column_types: Dict[str, str]) -> ValidationResult:
        """
        Validate data types of DataFrame columns.
        
        Args:
            df: DataFrame to validate
            column_types: Dictionary mapping column names to expected types
            
        Returns:
            ValidationResult containing any errors found
        """
        result = ValidationResult()
        
        for column, expected_type in column_types.items():
            if column not in df.columns:
                result.add_error(ValidationError(
                    column=column,
                    row_index=0,
                    value=None,
                    message=f"Column not found",
                    error_type="MissingColumn"
                ))
                continue
            
            validator = self.type_validators.get(expected_type)
            if not validator:
                result.add_error(ValidationError(
                    column=column,
                    row_index=0,
                    value=expected_type,
                    message=f"Unknown type validator: {expected_type}",
                    error_type="ValidatorError"
                ))
                continue
            
            # Validate each value in the column
            for idx, value in enumerate(df[column], start=1):
                if pd.isna(value):
                    continue  # Skip NA values
                
                try:
                    if not validator(value):
                        result.add_error(ValidationError(
                            column=column,
                            row_index=idx,
                            value=value,
                            message=f"Invalid {expected_type} value: {value}",
                            error_type="TypeError"
                        ))
                except Exception as e:
                    result.add_error(ValidationError(
                        column=column,
                        row_index=idx,
                        value=value,
                        message=f"Validation error: {str(e)}",
                        error_type="ValidationError"
                    ))
        
        return result
    
    def validate_custom_rules(self,
                            df: pd.DataFrame,
                            rules: Dict[str, Callable]) -> ValidationResult:
        """
        Apply custom validation rules to DataFrame.
        
        Args:
            df: DataFrame to validate
            rules: Dictionary mapping column names to validation functions
            
        Returns:
            ValidationResult containing any errors found
        """
        result = ValidationResult()
        
        for column, rule_func in rules.items():
            if column not in df.columns:
                result.add_error(ValidationError(
                    column=column,
                    row_index=0,
                    value=None,
                    message=f"Column not found",
                    error_type="MissingColumn"
                ))
                continue
            
            try:
                # Apply custom validation rule
                for idx, value in enumerate(df[column], start=1):
                    if pd.isna(value):
                        continue
                    
                    try:
                        if not rule_func(value):
                            result.add_error(ValidationError(
                                column=column,
                                row_index=idx,
                                value=value,
                                message=f"Failed custom validation rule",
                                error_type="CustomRuleError"
                            ))
                    except Exception as e:
                        result.add_error(ValidationError(
                            column=column,
                            row_index=idx,
                            value=value,
                            message=f"Custom rule error: {str(e)}",
                            error_type="CustomRuleError"
                        ))
            
            except Exception as e:
                result.add_error(ValidationError(
                    column=column,
                    row_index=0,
                    value=None,
                    message=f"Error applying custom rule: {str(e)}",
                    error_type="ValidationError"
                ))
        
        return result
    
    def register_custom_validator(self, name: str, validator: Callable) -> None:
        """
        Register a custom type validator.
        
        Args:
            name: Name of the validator
            validator: Validator function that takes a value and returns bool
        """
        self.type_validators[name] = validator
    
    def _validate_int(self, value: Any) -> bool:
        """Validate integer values."""
        if isinstance(value, (int, np.integer)):
            return True
        if isinstance(value, str):
            return value.strip().isdigit()
        return False
    
    def _validate_float(self, value: Any) -> bool:
        """Validate float values."""
        if isinstance(value, (float, np.floating, int, np.integer)):
            return True
        if isinstance(value, str):
            try:
                float(value.strip())
                return True
            except ValueError:
                return False
        return False
    
    def _validate_str(self, value: Any) -> bool:
        """Validate string values."""
        return isinstance(value, str)
    
    def _validate_date(self, value: Any) -> bool:
        """Validate date values."""
        if isinstance(value, (datetime, pd.Timestamp)):
            return True
        if isinstance(value, str):
            try:
                pd.to_datetime(value)
                return True
            except (ValueError, TypeError):
                return False
        return False
    
    def _validate_bool(self, value: Any) -> bool:
        """Validate boolean values."""
        if isinstance(value, (bool, np.bool_)):
            return True
        if isinstance(value, str):
            value = value.lower().strip()
            return value in ('true', 'false', '1', '0', 'yes', 'no')
        if isinstance(value, (int, float)):
            return value in (0, 1)
        return False 