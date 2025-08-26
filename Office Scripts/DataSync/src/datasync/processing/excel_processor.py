"""
Excel processing module for handling Excel file operations.
"""

import pandas as pd
from typing import Dict, List, Optional, Union
from pathlib import Path
import logging
import os

logger = logging.getLogger(__name__)

class ExcelProcessor:
    """Handles Excel file processing operations."""
    
    def __init__(self, file_path: Union[str, Path]):
        """
        Initialize ExcelProcessor with file path.
        
        Args:
            file_path: Path to the Excel file
        """
        self.file_path = Path(file_path)
        self._validate_file()
    
    def _validate_file(self) -> None:
        """Validate that the file exists and is an Excel file."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.file_path}")
        if not self.file_path.suffix.lower() in ['.xlsx', '.xls']:
            raise ValueError(f"Invalid file type: {self.file_path.suffix}")
    
    def read_sheet(self, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """
        Read data from an Excel sheet.
        
        Args:
            sheet_name: Name of the sheet to read. If None, reads the first sheet.
            
        Returns:
            DataFrame containing the sheet data
        """
        try:
            logger.info(f"Reading sheet '{sheet_name}' from {self.file_path}")
            
            # Handle .xls files with fallback strategies
            if self.file_path.suffix.lower() == '.xls':
                # Try xlrd first for .xls files
                try:
                    if sheet_name is None:
                        sheets = pd.read_excel(self.file_path, sheet_name=None, engine='xlrd')
                        first_sheet_name = list(sheets.keys())[0]
                        return sheets[first_sheet_name]
                    return pd.read_excel(self.file_path, sheet_name=sheet_name, engine='xlrd')
                except Exception as xlrd_error:
                    logger.warning(f"xlrd failed for .xls file: {xlrd_error}")
                    # Try openpyxl
                    try:
                        if sheet_name is None:
                            sheets = pd.read_excel(self.file_path, sheet_name=None, engine='openpyxl')
                            first_sheet_name = list(sheets.keys())[0]
                            return sheets[first_sheet_name]
                        return pd.read_excel(self.file_path, sheet_name=sheet_name, engine='openpyxl')
                    except Exception as openpyxl_error:
                        logger.warning(f"openpyxl also failed: {openpyxl_error}")
                        # Last resort: try to read as CSV/text file
                        logger.info("Attempting to read .xls file as CSV/text format")
                        try:
                            # Try different encodings and separators
                            for encoding in ['utf-16', 'utf-8', 'latin-1']:
                                for sep in ['\t', ',', ';']:
                                    try:
                                        df = pd.read_csv(self.file_path, encoding=encoding, sep=sep)
                                        if len(df.columns) > 1:  # Successfully parsed multiple columns
                                            logger.info(f"Successfully read as CSV with encoding={encoding}, sep='{sep}'")
                                            return df
                                    except:
                                        continue
                            # If all CSV attempts fail, raise the original error
                            raise xlrd_error
                        except Exception as csv_error:
                            logger.error(f"All reading methods failed: {csv_error}")
                            raise xlrd_error
            
            # Handle .xlsx files
            elif self.file_path.suffix.lower() == '.xlsx':
                engine = 'openpyxl'
                if sheet_name is None:
                    sheets = pd.read_excel(self.file_path, sheet_name=None, engine=engine)
                    first_sheet_name = list(sheets.keys())[0]
                    return sheets[first_sheet_name]
                return pd.read_excel(self.file_path, sheet_name=sheet_name, engine=engine)
            
            # Default behavior for other extensions
            else:
                if sheet_name is None:
                    sheets = pd.read_excel(self.file_path, sheet_name=None)
                    first_sheet_name = list(sheets.keys())[0]
                    return sheets[first_sheet_name]
                return pd.read_excel(self.file_path, sheet_name=sheet_name)
                
        except Exception as e:
            logger.error(f"Error reading Excel sheet: {str(e)}")
            raise
    
    def read_all_sheets(self) -> Dict[str, pd.DataFrame]:
        """
        Read all sheets from the Excel file.
        
        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        try:
            logger.info(f"Reading all sheets from {self.file_path}")
            
            # Determine the appropriate engine based on file extension
            engine = None
            if self.file_path.suffix.lower() == '.xls':
                engine = 'xlrd'
            elif self.file_path.suffix.lower() == '.xlsx':
                engine = 'openpyxl'
                
            return pd.read_excel(self.file_path, sheet_name=None, engine=engine)
        except Exception as e:
            logger.error(f"Error reading Excel sheets: {str(e)}")
            raise
    
    def write_sheet(self, 
                   data: pd.DataFrame, 
                   sheet_name: str, 
                   index: bool = False) -> None:
        """
        Write data to an Excel sheet.
        
        Args:
            data: DataFrame to write
            sheet_name: Name of the sheet to write to
            index: Whether to write the index
        """
        try:
            logger.info(f"Writing to sheet '{sheet_name}' in {self.file_path}")
            
            # Read existing sheets if file exists
            existing_sheets = {}
            if os.path.exists(self.file_path):
                try:
                    existing_sheets = pd.read_excel(self.file_path, sheet_name=None)
                except Exception:
                    pass
            
            # Update the sheet data
            existing_sheets[sheet_name] = data
            
            # Write all sheets back to the file
            with pd.ExcelWriter(self.file_path, engine='openpyxl') as writer:
                for name, df in existing_sheets.items():
                    df.to_excel(writer, sheet_name=name, index=index)
                    
        except Exception as e:
            logger.error(f"Error writing to Excel sheet: {str(e)}")
            raise
    
    def get_sheet_names(self) -> List[str]:
        """
        Get names of all sheets in the Excel file.
        
        Returns:
            List of sheet names
        """
        try:
            logger.info(f"Getting sheet names from {self.file_path}")
            
            # Try different engines/methods based on file extension
            if self.file_path.suffix.lower() == '.xls':
                # Try xlrd first for .xls files
                try:
                    return pd.ExcelFile(self.file_path, engine='xlrd').sheet_names
                except Exception as xlrd_error:
                    logger.warning(f"xlrd failed for .xls file: {xlrd_error}")
                    # If xlrd fails, try openpyxl (some .xls files are actually .xlsx)
                    try:
                        return pd.ExcelFile(self.file_path, engine='openpyxl').sheet_names
                    except Exception as openpyxl_error:
                        logger.warning(f"openpyxl also failed: {openpyxl_error}")
                        # Last resort: treat as CSV/text file
                        logger.info("Treating .xls file as CSV/text format")
                        return ["Sheet1"]  # Default sheet name for CSV-like files
            elif self.file_path.suffix.lower() == '.xlsx':
                return pd.ExcelFile(self.file_path, engine='openpyxl').sheet_names
            else:
                # Default behavior
                return pd.ExcelFile(self.file_path).sheet_names
                
        except Exception as e:
            logger.error(f"Error getting sheet names: {str(e)}")
            raise
    
    def validate_sheet_structure(self, 
                               sheet_name: str, 
                               required_columns: List[str]) -> bool:
        """
        Validate that a sheet has the required columns.
        
        Args:
            sheet_name: Name of the sheet to validate
            required_columns: List of required column names
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            df = self.read_sheet(sheet_name)
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                logger.warning(f"Missing columns in sheet '{sheet_name}': {missing_columns}")
                return False
            return True
        except Exception as e:
            logger.error(f"Error validating sheet structure: {str(e)}")
            return False
    
    def get_sheet_columns(self, sheet_name: str) -> List[str]:
        """
        Get column names from a specific sheet.
        
        Args:
            sheet_name: Name of the sheet
            
        Returns:
            List of column names
        """
        try:
            logger.info(f"Getting column names from sheet '{sheet_name}' in {self.file_path}")
            df = self.read_sheet(sheet_name)
            return list(df.columns)
        except Exception as e:
            logger.error(f"Error getting column names: {str(e)}")
            raise
    
    def is_column_empty(self, sheet_name: str, column_name: str) -> bool:
        """
        Check if a column in a sheet is empty (all values are NA).
        
        Args:
            sheet_name: Name of the sheet
            column_name: Name of the column to check
            
        Returns:
            True if the column is empty, False otherwise
        """
        try:
            logger.info(f"Checking if column '{column_name}' in sheet '{sheet_name}' is empty")
            df = self.read_sheet(sheet_name)
            if column_name not in df.columns:
                raise ValueError(f"Column '{column_name}' not found in sheet '{sheet_name}'")
            return df[column_name].isna().all()
        except Exception as e:
            logger.error(f"Error checking column emptiness: {str(e)}")
            raise 