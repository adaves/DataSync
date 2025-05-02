"""
Table operations for Access databases.
Handles table metadata, data reading, and filtering operations.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Callable
from pathlib import Path
import pyodbc
import pandas as pd
from functools import lru_cache
from .access_utils import access_connection, AccessDatabaseError
from .date_handling import DateFilter

@dataclass(frozen=True)
class ColumnInfo:
    """Information about a database column."""
    name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool = False
    character_maximum_length: Optional[int] = None

@dataclass(frozen=True)
class TableInfo:
    """Information about a database table."""
    name: str
    columns: List[ColumnInfo]

    def get_column(self, name: str) -> Optional[ColumnInfo]:
        """Get column info by name (case-insensitive)."""
        name_lower = name.lower()
        return next(
            (col for col in self.columns if col.name.lower() == name_lower),
            None
        )

    @property
    def column_names(self) -> List[str]:
        """Get list of column names."""
        return [col.name for col in self.columns]

@lru_cache(maxsize=128)
def get_table_info(db_path: Path, table_name: str) -> TableInfo:
    """
    Get metadata information about a table.
    
    Args:
        db_path (Path): Path to the Access database
        table_name (str): Name of the table to get info for
        
    Returns:
        TableInfo: Table metadata including columns and their types
        
    Raises:
        FileNotFoundError: If database file doesn't exist
        AccessDatabaseError: If table doesn't exist or other database error
    """
    with access_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # Check if table exists
        tables = [row.table_name for row in cursor.tables(tableType='TABLE')
                 if not row.table_name.startswith('MSys')]
        if table_name not in tables:
            raise AccessDatabaseError(f"Table not found: {table_name}")
        
        # Get column information
        columns = []
        for row in cursor.columns(table=table_name):
            # Skip system columns
            if row.column_name.startswith('MSys'):
                continue
                
            col_info = ColumnInfo(
                name=row.column_name,
                data_type=row.type_name,
                is_nullable=row.nullable == 1,
                character_maximum_length=row.column_size
            )
            columns.append(col_info)
            
        # Get primary key information
        try:
            for row in cursor.primaryKeys(table=table_name):
                # Find and update the column to mark it as primary key
                pk_col = next(col for col in columns 
                            if col.name == row.column_name)
                # Since ColumnInfo is frozen, we need to create a new instance
                pk_index = columns.index(pk_col)
                columns[pk_index] = ColumnInfo(
                    name=pk_col.name,
                    data_type=pk_col.data_type,
                    is_nullable=pk_col.is_nullable,
                    is_primary_key=True,
                    character_maximum_length=pk_col.character_maximum_length
                )
        except pyodbc.Error:
            # Some Access tables might not have explicit primary keys
            pass
            
        return TableInfo(name=table_name, columns=columns)

def read_filtered_data(
    db_path: Path,
    table_name: str,
    date_filter: DateFilter,
    chunk_size: int = 10000,
    progress_callback: Optional[Callable[[int, int], None]] = None
) -> pd.DataFrame:
    """
    Read and filter data from an Access table by date.
    
    Args:
        db_path: Path to the Access database
        table_name: Name of the table to read from
        date_filter: DateFilter object specifying the date range
        chunk_size: Number of rows to read at once (default: 10000)
        progress_callback: Optional callback for progress updates
        
    Returns:
        pd.DataFrame: Filtered data
        
    Raises:
        FileNotFoundError: If database file doesn't exist
        AccessDatabaseError: If table doesn't exist or other database error
        ValueError: If date column is invalid or missing
    """
    # Get table metadata
    table_info = get_table_info(db_path, table_name)
    
    # Find date column
    date_columns = [
        col for col in table_info.columns 
        if col.data_type.lower() in ('datetime', 'date')
    ]
    if not date_columns:
        raise ValueError(f"No date column found in table {table_name}")
    date_column = date_columns[0].name
    
    # Build query
    where_clause = date_filter.get_where_clause(date_column)
    query = f"SELECT * FROM {table_name} WHERE {where_clause}"
    
    # Read data in chunks
    chunks = []
    total_rows = 0
    
    with access_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # Get total count for progress tracking
        count_query = f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"
        cursor.execute(count_query)
        total_count = cursor.fetchone()[0]
        
        # Read data in chunks
        cursor.execute(query)
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
                
            # Convert to DataFrame
            chunk_df = pd.DataFrame.from_records(
                rows,
                columns=[col.name for col in table_info.columns]
            )
            chunks.append(chunk_df)
            
            # Update progress
            total_rows += len(rows)
            if progress_callback:
                progress_callback(total_rows, total_count)
    
    # Combine chunks
    if not chunks:
        return pd.DataFrame(columns=[col.name for col in table_info.columns])
    
    return pd.concat(chunks, ignore_index=True) 