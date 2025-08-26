"""
Helper functions module.
"""

from typing import Any, Dict, List, Union, Iterable
import pandas as pd
import datetime
from pathlib import Path

def format_timestamp(timestamp: datetime.datetime) -> str:
    """Format a timestamp as a string."""
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")

def safe_get(dictionary: Dict, key: str, default: Any = None) -> Any:
    """Safely get a value from a dictionary."""
    return dictionary.get(key, default)

def ensure_directory(directory: Path) -> None:
    """Ensure a directory exists."""
    directory.mkdir(parents=True, exist_ok=True)

def chunk_list(items: Iterable, chunk_size: int) -> List[List]:
    """
    Split an iterable into chunks of specified size.

    Args:
        items: Iterable to split
        chunk_size: Size of each chunk

    Returns:
        List of chunks, each containing up to chunk_size items
    """
    if not items:
        return []
    
    # Convert items to list to handle non-list iterables
    items_list = list(items)
    return [items_list[i:i + chunk_size] for i in range(0, len(items_list), chunk_size)]

def validate_dataframe(df: pd.DataFrame,
                      required_columns: List[str] = None,
                      column_types: Dict[str, Union[str, type]] = None) -> bool:
    """
    Validate a DataFrame's structure and data types.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        column_types: Dictionary mapping column names to expected types

    Returns:
        bool: True if validation passes, False otherwise
    """
    if df is None:
        return False
        
    if required_columns:
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            return False

    if column_types:
        for column, expected_type in column_types.items():
            if column not in df.columns:
                return False
                
            # Get non-null values for type checking
            non_null_values = df[column].dropna()
            if len(non_null_values) == 0:
                continue  # Skip empty columns
                
            if isinstance(expected_type, str):
                if not df[column].dtype == expected_type:
                    return False
            else:
                # Check if any non-null value is not of the expected type
                if not all(isinstance(x, expected_type) for x in non_null_values):
                    return False

    return True 