"""
Excel file validation module.
"""

import pandas as pd
from typing import Dict, List
from src.datasync.processing.excel_processor import ExcelProcessor

class ExcelValidator:
    """Handles Excel file validation operations."""
    
    def validate_sheet_structure(self, processor: ExcelProcessor, sheet_name: str, required_columns: List[str]) -> bool:
        """Validate that a sheet has the required columns."""
        try:
            df = processor.read_sheet(sheet_name)
            return all(col in df.columns for col in required_columns)
        except Exception:
            return False
    
    def validate_data_types(self, df: pd.DataFrame, expected_types: Dict[str, str]) -> bool:
        """Validate DataFrame column data types."""
        try:
            return all(df[col].dtype.name == dtype for col, dtype in expected_types.items())
        except Exception:
            return False 