"""
Excel validation module.
"""

import pandas as pd
from typing import Dict, List, Optional, Union
from pathlib import Path
from datasync.processing.excel_processor import ExcelProcessor

class ExcelValidator:
    """Handles Excel file validation."""
    
    def validate_sheet_structure(self, 
                               processor: ExcelProcessor,
                               sheet_name: str,
                               required_columns: List[str]) -> bool:
        """
        Validate sheet structure against required columns.
        
        Args:
            processor: ExcelProcessor instance
            sheet_name: Name of the sheet to validate
            required_columns: List of required column names
            
        Returns:
            True if validation passes, False otherwise
        """
        return processor.validate_sheet_structure(sheet_name, required_columns)
    
    def validate_data_types(self,
                          df: pd.DataFrame,
                          column_types: Dict[str, str]) -> bool:
        """
        Validate data types of DataFrame columns.
        
        Args:
            df: DataFrame to validate
            column_types: Dictionary mapping column names to expected types
            
        Returns:
            True if validation passes, False otherwise
        """
        for column, expected_type in column_types.items():
            if column not in df.columns:
                return False
            if df[column].dtype.name != expected_type:
                return False
        return True 