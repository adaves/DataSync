"""
Upload operations for Access databases.
Handles data uploading from pandas DataFrames to Access tables with proper type conversion.
"""

import pandas as pd
import numpy as np
import pyodbc
import logging
import time
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any, Tuple, Union

from .access_utils import access_connection, AccessDatabaseError
from .table_operations import get_table_info, TableInfo, ColumnInfo

logger = logging.getLogger(__name__)

@dataclass
class UploadResult:
    """Results of an upload operation."""
    success: bool
    rows_uploaded: int
    rows_skipped: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    elapsed_time: float = 0.0  # In seconds

    def __str__(self) -> str:
        """String representation of upload results."""
        parts = [
            f"success={self.success}",
            f"rows_uploaded={self.rows_uploaded}",
            f"rows_skipped={self.rows_skipped}",
            f"elapsed_time={self.elapsed_time:.2f}s",
        ]
        
        if self.errors:
            parts.append(f"errors=[{', '.join(self.errors[:3])}{'...' if len(self.errors) > 3 else ''}]")
        
        if self.warnings:
            parts.append(f"warnings=[{', '.join(self.warnings[:3])}{'...' if len(self.warnings) > 3 else ''}]")
        
        return "UploadResult(" + ", ".join(parts) + ")"


def upload_data_to_table(
    db_path: Path, 
    table_name: str, 
    df: pd.DataFrame, 
    batch_size: int = 1000,
    truncate_strings: bool = True,
    auto_generate_ids: bool = True,
    progress_callback: Optional[Callable[[int, int], None]] = None
) -> UploadResult:
    """
    Upload DataFrame data to Access table with progress reporting and batch processing.
    
    Args:
        db_path: Path to the Access database
        table_name: Name of the target table
        df: DataFrame with data to upload
        batch_size: Number of rows to upload in each batch (default: 1000)
        truncate_strings: Whether to truncate strings that exceed column length limits (default: True)
        auto_generate_ids: Whether to auto-generate ID columns if missing (default: True)
        progress_callback: Optional callback for progress reporting
            First argument is rows processed, second is total rows
    
    Returns:
        UploadResult with information about the upload operation
    """
    start_time = time.time()
    
    if len(df) == 0:
        return UploadResult(
            success=True,
            rows_uploaded=0,
            rows_skipped=0,
            elapsed_time=time.time() - start_time,
            warnings=["No data to upload (empty DataFrame)"]
        )
    
    try:
        # Get table metadata
        table_info = get_table_info(db_path, table_name)
        
        # Find missing ID columns that need to be auto-generated
        missing_id_columns = []
        if auto_generate_ids:
            # Case-insensitive column mapping
            df_cols = {col.lower(): col for col in df.columns}
            
            for col_info in table_info.columns:
                col_lower = col_info.name.lower()
                
                # Check if column is missing and is an ID column
                if col_lower not in df_cols and (
                    col_info.is_primary_key or 
                    col_lower == 'id' or 
                    col_lower.endswith('id')):
                    missing_id_columns.append(col_info)
        
        # Prepare data (convert types, truncate strings if needed)
        prepared_df = prepare_data_for_upload(df, table_info, truncate_strings)
        
        # Map column names to indices for efficient access
        excel_cols = {col.lower(): i for i, col in enumerate(prepared_df.columns)}
        access_cols = []
        
        for col_info in table_info.columns:
            col_lower = col_info.name.lower()
            if col_lower in excel_cols:
                access_cols.append((col_info.name, excel_cols[col_lower]))
            elif col_info in missing_id_columns:
                # Add missing ID columns to the list for SQL generation
                access_cols.append((col_info.name, None))  # None indicates auto-generated
        
        # Create SQL query
        column_names = [col[0] for col in access_cols]
        
        # For columns with auto-generated IDs, we need to generate appropriate SQL
        if missing_id_columns:
            insert_query = create_insert_query_with_autogen(table_name, column_names, 
                                                          [col.name for col in missing_id_columns])
        else:
            insert_query = create_insert_query(table_name, column_names)
        
        # Calculate number of batches
        total_rows = len(prepared_df)
        num_batches = (total_rows + batch_size - 1) // batch_size
        
        rows_uploaded = 0
        warnings = []
        
        with access_connection(db_path) as conn:
            cursor = conn.cursor()
            
            try:
                # If we need to auto-generate IDs, get current max values
                id_start_values = {}
                if missing_id_columns:
                    for col_info in missing_id_columns:
                        try:
                            # Get the current max value for the ID column
                            max_query = f"SELECT MAX([{col_info.name}]) FROM [{table_name}]"
                            cursor.execute(max_query)
                            max_val = cursor.fetchone()[0]
                            
                            # Start from max + 1, or 1 if no data exists
                            id_start_values[col_info.name] = (max_val or 0) + 1
                            warnings.append(f"Auto-generating {col_info.name} starting from {id_start_values[col_info.name]}")
                        except Exception as e:
                            logger.warning(f"Could not determine start value for auto-generated column {col_info.name}: {e}")
                            id_start_values[col_info.name] = 1
                
                # Process in batches for large datasets
                for batch_index in range(num_batches):
                    start_row = batch_index * batch_size
                    end_row = min(start_row + batch_size, total_rows)
                    batch = prepared_df.iloc[start_row:end_row]
                    
                    # Upload each row in the batch
                    for row_idx, row in enumerate(batch.itertuples(index=False), start=start_row):
                        # Extract values in the correct order for the INSERT statement
                        values = []
                        for col_name, col_idx in access_cols:
                            if col_idx is not None:
                                # Regular column from Excel data
                                values.append(row[col_idx])
                            else:
                                # Auto-generated ID column
                                id_value = id_start_values.get(col_name, 1) + row_idx
                                values.append(id_value)
                        
                        # Execute the INSERT
                        cursor.execute(insert_query, values)
                        # Count successful inserts correctly - testing showed each batch adds cursor.rowcount
                        rows_uploaded += 1  # Assume one row per insert to match cursor.execute call count
                    
                    # Commit each batch
                    conn.commit()
                    
                    # Report progress
                    if progress_callback:
                        progress_callback(end_row, total_rows)
                
                elapsed_time = time.time() - start_time
                
                return UploadResult(
                    success=True,
                    rows_uploaded=rows_uploaded,
                    rows_skipped=0,
                    warnings=warnings,
                    elapsed_time=elapsed_time
                )
                
            except Exception as e:
                # Rollback on error
                conn.rollback()
                logger.error(f"Error uploading data: {str(e)}")
                
                elapsed_time = time.time() - start_time
                
                return UploadResult(
                    success=False,
                    rows_uploaded=0,
                    rows_skipped=total_rows,
                    errors=[f"Database error: {str(e)}"],
                    elapsed_time=elapsed_time
                )
        
    except AccessDatabaseError as e:
        elapsed_time = time.time() - start_time
        return UploadResult(
            success=False,
            rows_uploaded=0,
            rows_skipped=len(df),
            errors=[f"Database access error: {str(e)}"],
            elapsed_time=elapsed_time
        )
    except Exception as e:
        elapsed_time = time.time() - start_time
        return UploadResult(
            success=False,
            rows_uploaded=0,
            rows_skipped=len(df),
            errors=[f"Unexpected error: {str(e)}"],
            elapsed_time=elapsed_time
        )


def prepare_data_for_upload(
    df: pd.DataFrame,
    table_info: TableInfo,
    truncate_strings: bool = True
) -> pd.DataFrame:
    """
    Convert DataFrame data types to match Access table requirements.
    
    Args:
        df: DataFrame with data to convert
        table_info: Table metadata
        truncate_strings: Whether to truncate strings that exceed column length limits
        
    Returns:
        New DataFrame with converted data types
    """
    # Make a copy to avoid modifying the original
    result = df.copy()
    
    # Case-insensitive column mapping
    df_cols = {col.lower(): col for col in df.columns}
    
    # Problem columns that should be treated as text
    treat_as_text = [
        'direct projected volume', 
        'projected gm %', 
        'budget cot', 
        'projected volume', 
        'net qty (cases)', 
        'planned volume'
    ]
    
    for col_info in table_info.columns:
        col_lower = col_info.name.lower()
        
        # Skip columns not in the DataFrame
        if col_lower not in df_cols:
            continue
        
        df_col = df_cols[col_lower]
        
        # Check if this is a column that should be treated as text
        if col_lower in treat_as_text:
            # Force text conversion for these columns regardless of database type
            try:
                result[df_col] = result[df_col].astype(str)
                
                # Truncate strings if they exceed the maximum length
                if truncate_strings and col_info.character_maximum_length:
                    max_len = col_info.character_maximum_length
                    mask = result[df_col].str.len() > max_len
                    if mask.any():
                        result.loc[mask, df_col] = result.loc[mask, df_col].str.slice(0, max_len)
            except Exception as e:
                logger.warning(f"Error converting column {df_col} to text: {str(e)}")
            continue
        
        # Convert types based on Access column type
        if col_info.data_type.lower() in ('short', 'long', 'integer', 'byte', 'int'):
            # Convert to integer
            try:
                result[df_col] = pd.to_numeric(result[df_col], errors='coerce').fillna(0).astype(int)
            except Exception as e:
                logger.warning(f"Error converting column {df_col} to integer: {str(e)}")
        
        elif col_info.data_type.lower() in ('double', 'single', 'decimal', 'float', 'real', 'number'):
            # Convert to float
            try:
                result[df_col] = pd.to_numeric(result[df_col], errors='coerce')
            except Exception as e:
                logger.warning(f"Error converting column {df_col} to float: {str(e)}")
        
        elif col_info.data_type.lower() in ('date', 'date/time', 'datetime'):
            # Convert to datetime
            try:
                result[df_col] = pd.to_datetime(result[df_col], errors='coerce')
            except Exception as e:
                logger.warning(f"Error converting column {df_col} to datetime: {str(e)}")
        
        elif col_info.data_type.lower() in ('text', 'char', 'varchar', 'longchar', 'string', 'memo'):
            # Convert to string and truncate if needed
            try:
                result[df_col] = result[df_col].astype(str)
                
                # Truncate strings if they exceed the maximum length
                if truncate_strings and col_info.character_maximum_length:
                    max_len = col_info.character_maximum_length
                    
                    # Apply truncation where needed
                    mask = result[df_col].str.len() > max_len
                    if mask.any():
                        result.loc[mask, df_col] = result.loc[mask, df_col].str.slice(0, max_len)
                
            except Exception as e:
                logger.warning(f"Error processing column {df_col} as string: {str(e)}")
        
        elif col_info.data_type.lower() in ('bit', 'boolean', 'logical', 'yes/no'):
            # Convert to boolean
            try:
                # Handle various boolean representations
                bool_map = {
                    'true': True, 't': True, 'yes': True, 'y': True, '1': True, 1: True,
                    'false': False, 'f': False, 'no': False, 'n': False, '0': False, 0: False
                }
                
                # Convert to lowercase for string values
                temp_series = result[df_col].copy()
                str_mask = temp_series.apply(lambda x: isinstance(x, str))
                if str_mask.any():
                    temp_series.loc[str_mask] = temp_series.loc[str_mask].str.lower()
                
                # Map values to booleans
                result[df_col] = temp_series.map(bool_map)
                
            except Exception as e:
                logger.warning(f"Error converting column {df_col} to boolean: {str(e)}")
    
    # Replace NaN values with None for SQL compatibility
    result = result.where(pd.notnull(result), None)
    
    return result


def create_insert_query(
    table_name: str,
    column_names: List[str]
) -> str:
    """
    Generate a parameterized INSERT query for the given table and columns.
    
    Args:
        table_name: Name of the target table
        column_names: List of column names to include in the INSERT
        
    Returns:
        SQL query string with parameterized values
    """
    columns_str = ", ".join([f"[{col}]" for col in column_names])
    placeholders = ", ".join(["?"] * len(column_names))
    
    return f"INSERT INTO [{table_name}] ({columns_str}) VALUES ({placeholders})"


def create_insert_query_with_autogen(
    table_name: str,
    column_names: List[str],
    auto_columns: List[str]
) -> str:
    """
    Create SQL INSERT query that handles auto-generated columns.
    
    Args:
        table_name: Name of the target table
        column_names: List of column names
        auto_columns: List of auto-generated column names
    
    Returns:
        SQL query string
    """
    # Format column names with brackets for Access SQL
    formatted_columns = [f"[{col}]" for col in column_names]
    
    # Create placeholders for values (?)
    placeholders = ["?"] * len(column_names)
    
    # Construct the query
    query = f"INSERT INTO [{table_name}] ({', '.join(formatted_columns)}) VALUES ({', '.join(placeholders)})"
    
    return query 