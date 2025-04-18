"""
Helper functions module.
"""

from typing import Any, Dict, List, Union
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

def chunk_list(items: List, chunk_size: int) -> List[List]:
    """Split a list into chunks of specified size."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

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
        True if validation passes, False otherwise
    """
    if required_columns:
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            return False
    
    if column_types:
        for column, expected_type in column_types.items():
            if column not in df.columns:
                continue
                
            if isinstance(expected_type, str):
                # Handle string type specifications (e.g., 'int64', 'float64')
                if not df[column].dtype.name == expected_type:
                    return False
            else:
                # Handle Python type objects
                if not all(isinstance(x, expected_type) for x in df[column]):
                    return False
    
    return True 