"""
Data validation utilities for Excel data before importing to Access.
Handles validation against table schema, data type checking, and constraint validation.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set, Tuple
import logging
from datetime import datetime

from app.database.table_operations import TableInfo, ColumnInfo

logger = logging.getLogger(__name__)

@dataclass
class ValidationError:
    """Represents a validation error that blocks import."""
    error_type: str  # e.g., "missing_column", "type_mismatch", "duplicate_key"
    message: str
    column_name: Optional[str] = None
    row_indices: Optional[List[int]] = None
    details: Optional[Dict[str, Any]] = None


@dataclass
class ValidationWarning:
    """Represents a validation issue that doesn't block import but might need attention."""
    warning_type: str  # e.g., "truncation", "potential_duplicate"
    message: str
    column_name: Optional[str] = None
    row_indices: Optional[List[int]] = None
    details: Optional[Dict[str, Any]] = None


@dataclass
class ValidationResult:
    """Container for validation results with errors and warnings."""
    is_valid: bool = True
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationWarning] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)

    def add_error(self, error: ValidationError) -> None:
        """Add an error and mark the validation as invalid."""
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: ValidationWarning) -> None:
        """Add a warning."""
        self.warnings.append(warning)

    def merge(self, other: 'ValidationResult') -> None:
        """Merge another validation result into this one."""
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        self.is_valid = self.is_valid and other.is_valid
        
        # Merge stats
        for key, value in other.stats.items():
            if key in self.stats:
                if isinstance(value, (int, float)) and isinstance(self.stats[key], (int, float)):
                    self.stats[key] += value
                else:
                    self.stats[key] = value
            else:
                self.stats[key] = value


def validate_data_for_table(
    df: pd.DataFrame, 
    table_info: TableInfo,
    validation_options: Optional[Dict[str, Any]] = None
) -> ValidationResult:
    """
    Master validation function that performs all checks.
    
    Args:
        df: DataFrame with data to validate
        table_info: Target table metadata
        validation_options: Optional configuration for validation
            - ignore_extra_columns: Whether to ignore extra columns in DataFrame (default: False)
            - truncate_strings: Whether to warn about strings that will be truncated (default: True)
            - check_duplicates: Whether to check for duplicate primary keys (default: True)
            - max_error_rows: Maximum number of row indices to include in errors (default: 10)
            
    Returns:
        ValidationResult with validation outcome including errors and warnings
    """
    options = {
        'ignore_extra_columns': False,
        'truncate_strings': True,
        'check_duplicates': True,
        'max_error_rows': 10,
    }
    
    if validation_options:
        options.update(validation_options)
    
    result = ValidationResult()
    result.stats['row_count'] = len(df)
    result.stats['column_count'] = len(df.columns)
    
    # Column validation
    column_result = check_required_columns(df, table_info, options)
    result.merge(column_result)
    
    # If missing required columns, we can't proceed with data validation
    if not result.is_valid:
        return result
    
    # Data type validation
    type_result = validate_data_types(df, table_info, options)
    result.merge(type_result)
    
    # Duplicate check (primary keys)
    if options['check_duplicates']:
        duplicate_result = check_for_duplicates(df, table_info, options)
        result.merge(duplicate_result)
    
    # Value constraint validation
    constraint_result = validate_value_constraints(df, table_info, options)
    result.merge(constraint_result)
    
    return result


def check_required_columns(
    df: pd.DataFrame, 
    table_info: TableInfo,
    options: Optional[Dict[str, Any]] = None
) -> ValidationResult:
    """
    Check if all required columns exist in the DataFrame.
    
    Args:
        df: DataFrame to validate
        table_info: Target table metadata
        options: Validation options
        
    Returns:
        ValidationResult with column validation result
    """
    result = ValidationResult()
    
    # Case-insensitive column mapping (Excel columns to Access columns)
    excel_cols = {col.lower(): col for col in df.columns}
    access_cols = {col.name.lower(): col for col in table_info.columns}
    
    # Check for missing required columns
    missing_cols = []
    missing_id_cols = []
    
    for col_name_lower, col_info in access_cols.items():
        if not col_info.is_nullable and col_name_lower not in excel_cols:
            # Check if it's an ID column (usually primary key or named "ID")
            if (col_info.is_primary_key or 
                col_name_lower == 'id' or 
                col_name_lower.endswith('id')):
                missing_id_cols.append(col_info.name)
            else:
                missing_cols.append(col_info.name)
    
    # Handle regular missing columns (not IDs)
    if missing_cols:
        result.add_error(ValidationError(
            error_type='missing_required_columns',
            message=f"Missing required columns: {', '.join(missing_cols)}",
            details={'columns': missing_cols}
        ))
    
    # Add missing ID columns as a warning instead of an error
    if missing_id_cols:
        result.add_warning(ValidationWarning(
            warning_type='missing_id_columns',
            message=f"Missing ID columns that will be auto-generated: {', '.join(missing_id_cols)}",
            details={'columns': missing_id_cols}
        ))
    
    # Check for extra columns
    if not options or not options.get('ignore_extra_columns', False):
        extra_cols = []
        for col_name_lower, excel_col in excel_cols.items():
            if col_name_lower not in access_cols:
                extra_cols.append(excel_col)
        
        if extra_cols:
            result.add_warning(ValidationWarning(
                warning_type='extra_columns',
                message=f"Excel file contains extra columns not in Access table: {', '.join(extra_cols)}",
                details={'columns': extra_cols}
            ))
    
    # Add stats
    result.stats['matching_columns'] = len(set(excel_cols.keys()) & set(access_cols.keys()))
    result.stats['missing_columns'] = len(missing_cols)
    result.stats['missing_id_columns'] = len(missing_id_cols)
    
    return result


def validate_data_types(
    df: pd.DataFrame, 
    table_info: TableInfo,
    options: Optional[Dict[str, Any]] = None
) -> ValidationResult:
    """
    Validate that data types in Excel match expected types in Access.
    
    Args:
        df: DataFrame to validate
        table_info: Target table metadata
        options: Validation options
            - treat_as_text: List of column names to treat as text regardless of their database type
        
    Returns:
        ValidationResult with data type validation result
    """
    result = ValidationResult()
    max_error_rows = options.get('max_error_rows', 10) if options else 10
    
    # Get columns to treat as text
    treat_as_text = options.get('treat_as_text', []) if options else []
    
    # Add problem columns that are commonly formatted as text but have numeric data types
    common_text_columns = [
        'direct projected volume', 
        'projected gm %', 
        'budget cot', 
        'projected volume', 
        'net qty (cases)', 
        'planned volume'
    ]
    treat_as_text.extend(common_text_columns)
    
    # Convert to lowercase for case-insensitive comparison
    treat_as_text = [col.lower() for col in treat_as_text]
    
    # Case-insensitive column mapping
    excel_cols = {col.lower(): col for col in df.columns}
    
    # Type validation by column
    for col_info in table_info.columns:
        col_name_lower = col_info.name.lower()
        
        # Skip columns not in Excel data
        if col_name_lower not in excel_cols:
            continue
        
        excel_col = excel_cols[col_name_lower]
        column_data = df[excel_col]
        
        # Skip type validation for columns specified to be treated as text
        if col_name_lower in treat_as_text:
            continue
        
        # Detect and validate types
        if col_info.data_type.lower() in ('short', 'long', 'integer', 'byte', 'int'):
            # Integer validation
            if not pd.api.types.is_integer_dtype(column_data):
                # Check if values can be converted to integers
                invalid_rows = []
                for idx, val in enumerate(column_data):
                    if pd.isna(val):
                        if not col_info.is_nullable:
                            invalid_rows.append(idx)
                    else:
                        try:
                            int(val)
                        except (ValueError, TypeError):
                            invalid_rows.append(idx)
                
                if invalid_rows:
                    result.add_error(ValidationError(
                        error_type='type_mismatch',
                        message=f"Column '{excel_col}' should contain integer values",
                        column_name=excel_col,
                        row_indices=invalid_rows[:max_error_rows],
                        details={'expected_type': 'integer', 
                                 'total_invalid': len(invalid_rows)}
                    ))
        
        elif col_info.data_type.lower() in ('double', 'single', 'decimal', 'float', 'real', 'number'):
            # Float validation
            if not pd.api.types.is_numeric_dtype(column_data):
                # Check if values can be converted to floats
                invalid_rows = []
                for idx, val in enumerate(column_data):
                    if pd.isna(val):
                        if not col_info.is_nullable:
                            invalid_rows.append(idx)
                    else:
                        try:
                            float(val)
                        except (ValueError, TypeError):
                            invalid_rows.append(idx)
                
                if invalid_rows:
                    result.add_error(ValidationError(
                        error_type='type_mismatch',
                        message=f"Column '{excel_col}' should contain numeric values",
                        column_name=excel_col,
                        row_indices=invalid_rows[:max_error_rows],
                        details={'expected_type': 'numeric', 
                                 'total_invalid': len(invalid_rows)}
                    ))
        
        elif col_info.data_type.lower() in ('date', 'date/time', 'datetime'):
            # Date validation
            if not pd.api.types.is_datetime64_dtype(column_data):
                # Check if values can be converted to dates
                invalid_rows = []
                for idx, val in enumerate(column_data):
                    if pd.isna(val):
                        if not col_info.is_nullable:
                            invalid_rows.append(idx)
                    else:
                        try:
                            pd.to_datetime(val)
                        except (ValueError, TypeError):
                            invalid_rows.append(idx)
                
                if invalid_rows:
                    result.add_error(ValidationError(
                        error_type='type_mismatch',
                        message=f"Column '{excel_col}' should contain date/time values",
                        column_name=excel_col,
                        row_indices=invalid_rows[:max_error_rows],
                        details={'expected_type': 'date', 
                                 'total_invalid': len(invalid_rows)}
                    ))
        
        elif col_info.data_type.lower() in ('text', 'char', 'varchar', 'longchar', 'string', 'memo'):
            # Text validation
            max_length = col_info.character_maximum_length
            
            # Check if string length exceeds the maximum
            if max_length and options and options.get('truncate_strings', True):
                too_long_rows = []
                for idx, val in enumerate(column_data):
                    if pd.notna(val) and len(str(val)) > max_length:
                        too_long_rows.append(idx)
                
                if too_long_rows:
                    result.add_warning(ValidationWarning(
                        warning_type='string_truncation',
                        message=f"Values in column '{excel_col}' exceed max length ({max_length})",
                        column_name=excel_col,
                        row_indices=too_long_rows[:max_error_rows],
                        details={'max_length': max_length, 
                                 'total_truncated': len(too_long_rows)}
                    ))
        
        elif col_info.data_type.lower() in ('bit', 'boolean', 'logical', 'yes/no'):
            # Boolean validation
            invalid_rows = []
            for idx, val in enumerate(column_data):
                if pd.isna(val):
                    if not col_info.is_nullable:
                        invalid_rows.append(idx)
                else:
                    val_str = str(val).lower()
                    if val_str not in ('true', 'false', 'yes', 'no', '1', '0', 't', 'f', 'y', 'n'):
                        invalid_rows.append(idx)
            
            if invalid_rows:
                result.add_error(ValidationError(
                    error_type='type_mismatch',
                    message=f"Column '{excel_col}' should contain boolean values",
                    column_name=excel_col,
                    row_indices=invalid_rows[:max_error_rows],
                    details={'expected_type': 'boolean', 
                             'total_invalid': len(invalid_rows)}
                ))
    
    return result


def check_for_duplicates(
    df: pd.DataFrame, 
    table_info: TableInfo,
    options: Optional[Dict[str, Any]] = None
) -> ValidationResult:
    """
    Check for duplicate primary keys or unique constraints.
    
    Args:
        df: DataFrame to validate
        table_info: Target table metadata
        options: Validation options
        
    Returns:
        ValidationResult with duplicate check result
    """
    result = ValidationResult()
    max_error_rows = options.get('max_error_rows', 10) if options else 10
    
    # Find primary key columns
    pk_cols = [col.name for col in table_info.columns if col.is_primary_key]
    
    if not pk_cols:
        # No primary key columns to check
        return result
    
    # Case-insensitive column mapping
    excel_cols = {col.lower(): col for col in df.columns}
    
    # Map Access PK columns to Excel columns
    excel_pk_cols = []
    missing_pk_cols = []
    
    for pk_col in pk_cols:
        pk_lower = pk_col.lower()
        if pk_lower in excel_cols:
            excel_pk_cols.append(excel_cols[pk_lower])
        else:
            missing_pk_cols.append(pk_col)
    
    if missing_pk_cols:
        # Already reported in column validation
        return result
    
    # Check for duplicates
    if excel_pk_cols:
        duplicates = df.duplicated(subset=excel_pk_cols, keep='first')
        duplicate_indices = duplicates[duplicates].index.tolist()
        
        if duplicate_indices:
            # Get some sample values
            pk_values = []
            for idx in duplicate_indices[:max_error_rows]:
                pk_value = tuple(df.loc[idx, col] for col in excel_pk_cols)
                pk_values.append(str(pk_value))
            
            result.add_error(ValidationError(
                error_type='duplicate_key',
                message=f"Found {len(duplicate_indices)} duplicate values for primary key columns: {', '.join(excel_pk_cols)}",
                row_indices=duplicate_indices[:max_error_rows],
                details={
                    'columns': excel_pk_cols,
                    'total_duplicates': len(duplicate_indices),
                    'sample_values': pk_values
                }
            ))
    
    return result


def validate_value_constraints(
    df: pd.DataFrame, 
    table_info: TableInfo,
    options: Optional[Dict[str, Any]] = None
) -> ValidationResult:
    """
    Validate that values meet constraints (e.g., ranges, not null).
    
    Args:
        df: DataFrame to validate
        table_info: Target table metadata
        options: Validation options
        
    Returns:
        ValidationResult with constraint validation result
    """
    result = ValidationResult()
    max_error_rows = options.get('max_error_rows', 10) if options else 10
    
    # Case-insensitive column mapping
    excel_cols = {col.lower(): col for col in df.columns}
    
    # Check for nulls in non-nullable columns
    for col_info in table_info.columns:
        col_name_lower = col_info.name.lower()
        
        # Skip columns not in Excel data
        if col_name_lower not in excel_cols:
            continue
        
        excel_col = excel_cols[col_name_lower]
        
        # Check for nulls in non-nullable columns
        if not col_info.is_nullable:
            null_indices = df[pd.isna(df[excel_col])].index.tolist()
            
            if null_indices:
                result.add_error(ValidationError(
                    error_type='null_in_non_nullable',
                    message=f"Column '{excel_col}' has {len(null_indices)} null values but does not allow nulls",
                    column_name=excel_col,
                    row_indices=null_indices[:max_error_rows],
                    details={'total_nulls': len(null_indices)}
                ))
    
    return result 