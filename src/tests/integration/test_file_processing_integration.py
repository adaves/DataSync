"""
Integration tests for file processing functionality.
"""

import pytest
import pandas as pd
import os
from pathlib import Path
from src.datasync.processing.excel_processor import ExcelProcessor
from src.datasync.processing.file_manager import FileManager
from src.datasync.processing.validation import ExcelValidator

@pytest.fixture
def test_data_dir(tmp_path):
    """Create a temporary directory for test data."""
    data_dir = tmp_path / "test_data"
    data_dir.mkdir()
    return data_dir

@pytest.fixture
def sample_excel_files(test_data_dir):
    """Create sample Excel files for testing."""
    # Create a simple Excel file
    simple_file = test_data_dir / "simple.xlsx"
    df1 = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [10, 20, 30]
    })
    df1.to_excel(simple_file, index=False)
    
    # Create a complex Excel file with multiple sheets
    complex_file = test_data_dir / "complex.xlsx"
    with pd.ExcelWriter(complex_file) as writer:
        df1.to_excel(writer, sheet_name='Sheet1', index=False)
        df2 = pd.DataFrame({
            'id': [4, 5, 6],
            'description': ['X', 'Y', 'Z'],
            'amount': [100, 200, 300]
        })
        df2.to_excel(writer, sheet_name='Sheet2', index=False)
    
    return {
        'simple': simple_file,
        'complex': complex_file
    }

class TestExcelProcessingIntegration:
    """Integration tests for Excel processing functionality."""
    
    def test_excel_processor_with_file_manager(self, sample_excel_files):
        """Test ExcelProcessor integration with FileManager."""
        # Initialize components
        file_manager = FileManager()
        processor = ExcelProcessor(sample_excel_files['complex'])
        
        # Test file operations
        assert file_manager.file_exists(sample_excel_files['complex'])
        assert file_manager.get_file_size(sample_excel_files['complex']) > 0
        
        # Test Excel processing
        sheets = processor.read_all_sheets()
        assert len(sheets) == 2
        assert 'Sheet1' in sheets
        assert 'Sheet2' in sheets
    
    def test_excel_validation(self, sample_excel_files):
        """Test Excel validation integration."""
        validator = ExcelValidator()
        processor = ExcelProcessor(sample_excel_files['complex'])
        
        # Test sheet validation
        assert validator.validate_sheet_structure(processor, 'Sheet1', ['id', 'name', 'value'])
        assert validator.validate_sheet_structure(processor, 'Sheet2', ['id', 'description', 'amount'])
        
        # Test data validation
        df = processor.read_sheet('Sheet1')
        assert validator.validate_data_types(df, {
            'id': 'int64',
            'name': 'object',
            'value': 'int64'
        })
    
    def test_complete_workflow(self, test_data_dir, sample_excel_files):
        """Test complete Excel processing workflow."""
        # Initialize components
        file_manager = FileManager()
        processor = ExcelProcessor(sample_excel_files['complex'])
        validator = ExcelValidator()
        
        # Read and validate data
        sheets = processor.read_all_sheets()
        assert validator.validate_sheet_structure(processor, 'Sheet1', ['id', 'name', 'value'])
        
        # Process data
        df = sheets['Sheet1']
        df['processed'] = df['value'] * 2
        
        # Write processed data to new file
        output_file = test_data_dir / "processed.xlsx"
        processor.write_sheet(df, 'Processed', output_file)
        
        # Verify output
        assert file_manager.file_exists(output_file)
        new_processor = ExcelProcessor(output_file)
        processed_df = new_processor.read_sheet('Processed')
        assert 'processed' in processed_df.columns
        assert all(processed_df['processed'] == df['value'] * 2)
    
    def test_error_handling(self, test_data_dir):
        """Test error handling in the processing workflow."""
        # Test with non-existent file
        with pytest.raises(FileNotFoundError):
            ExcelProcessor(test_data_dir / "nonexistent.xlsx")
        
        # Test with invalid file type
        invalid_file = test_data_dir / "invalid.txt"
        invalid_file.touch()
        with pytest.raises(ValueError):
            ExcelProcessor(invalid_file)
        
        # Test with corrupted Excel file
        corrupted_file = test_data_dir / "corrupted.xlsx"
        corrupted_file.write_bytes(b'invalid content')
        with pytest.raises(Exception):
            ExcelProcessor(corrupted_file) 