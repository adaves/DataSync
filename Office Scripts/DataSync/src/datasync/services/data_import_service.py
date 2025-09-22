"""
Data import service with automatic file discovery and one-by-one processing.
"""

import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import sys
import os

# Add src to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(os.path.dirname(current_dir))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from datasync.utils.file_discovery import FileDiscovery
from datasync.processing.excel_processor import ExcelProcessor
from src.database.operations import DatabaseOperations

logger = logging.getLogger(__name__)

class DataImportService:
    """Service for handling automatic data import with file discovery and processing."""
    
    def __init__(self, 
                 database_path: str,
                 target_table: str = "tblProjectedDataPTP",
                 data_dir: str = "data",
                 loaded_dir: str = "data/loaded"):
        """
        Initialize the data import service.
        
        Args:
            database_path: Path to the Access database
            target_table: Target table for importing data
            data_dir: Directory where new files are placed
            loaded_dir: Directory where processed files are moved
        """
        self.database_path = database_path
        self.target_table = target_table
        self.file_discovery = FileDiscovery(data_dir, loaded_dir)
        
        # Import statistics
        self.import_stats = {
            'files_processed': 0,
            'total_records_imported': 0,
            'total_errors': 0,
            'files_with_errors': [],
            'processing_time': 0.0
        }
    
    def get_status(self) -> Dict:
        """
        Get current status of the data import system.
        
        Returns:
            Dictionary with status information
        """
        directory_status = self.file_discovery.get_data_directory_status()
        
        # Add database connectivity check
        try:
            db_ops = DatabaseOperations(self.database_path)
            database_connected = db_ops.connect()
            if database_connected:
                db_ops.close()
        except Exception as e:
            database_connected = False
            logger.error(f"Database connection check failed: {e}")
        
        status = {
            'database_path': self.database_path,
            'target_table': self.target_table,
            'database_connected': database_connected,
            'file_discovery_status': directory_status,
            'ready_for_import': (
                directory_status.get('has_new_files', False) and 
                database_connected and 
                directory_status.get('directories_exist', False)
            ),
            'import_stats': self.import_stats.copy()
        }
        
        return status
    
    def discover_and_import(self, 
                          batch_size: int = 1000,
                          move_processed: bool = True,
                          skip_validation: bool = False) -> Dict:
        """
        Automatically discover and import new files using one-by-one method.
        
        Args:
            batch_size: Number of records to process in each database batch
            move_processed: Whether to move processed files to loaded directory
            skip_validation: Skip file validation (for testing)
            
        Returns:
            Dictionary with import results
        """
        import time
        start_time = time.time()
        
        # Reset stats
        self.import_stats = {
            'files_processed': 0,
            'total_records_imported': 0,
            'total_errors': 0,
            'files_with_errors': [],
            'processing_time': 0.0
        }
        
        try:
            # Discover new files
            new_files = self.file_discovery.discover_new_files()
            
            if not new_files:
                logger.info("No new files found for import")
                self.import_stats['processing_time'] = time.time() - start_time
                return {
                    'success': True,
                    'message': 'No new files found for import',
                    'files_processed': 0,
                    'stats': self.import_stats
                }
            
            logger.info(f"Found {len(new_files)} files to process: {[f.name for f in new_files]}")
            
            # Connect to database
            db_ops = DatabaseOperations(self.database_path)
            if not db_ops.connect():
                error_msg = "Failed to connect to database"
                logger.error(error_msg)
                return {
                    'success': False,
                    'message': error_msg,
                    'stats': self.import_stats
                }
            
            try:
                # Get table schema for data conversion
                table_schema = db_ops.get_table_schema(self.target_table)
                
                # Process files one by one (optimal method based on testing)
                successful_files = []
                
                for file_path in new_files:
                    logger.info(f"Processing file: {file_path.name}")
                    
                    try:
                        # Validate file if not skipping
                        if not skip_validation:
                            if not self.file_discovery.validate_file_for_processing(file_path):
                                logger.error(f"File validation failed: {file_path.name}")
                                self.import_stats['files_with_errors'].append({
                                    'file': file_path.name,
                                    'error': 'File validation failed'
                                })
                                continue
                        
                        # Process the file
                        import_result = self._process_single_file(
                            file_path, db_ops, table_schema, batch_size
                        )
                        
                        if import_result['success']:
                            self.import_stats['files_processed'] += 1
                            self.import_stats['total_records_imported'] += import_result['records_imported']
                            successful_files.append(file_path)
                            logger.info(f"Successfully processed {file_path.name}: {import_result['records_imported']} records")
                        else:
                            self.import_stats['files_with_errors'].append({
                                'file': file_path.name,
                                'error': import_result['error']
                            })
                            logger.error(f"Failed to process {file_path.name}: {import_result['error']}")
                    
                    except Exception as e:
                        error_msg = f"Unexpected error processing {file_path.name}: {str(e)}"
                        logger.error(error_msg)
                        self.import_stats['files_with_errors'].append({
                            'file': file_path.name,
                            'error': str(e)
                        })
                
                # Move successfully processed files
                if move_processed and successful_files:
                    moved_files = []
                    for file_path in successful_files:
                        moved_path = self.file_discovery.move_processed_file(file_path)
                        if moved_path:
                            moved_files.append(moved_path.name)
                        else:
                            logger.warning(f"Failed to move processed file: {file_path.name}")
                    
                    logger.info(f"Moved {len(moved_files)} processed files to loaded directory")
            
            finally:
                db_ops.close()
            
            # Calculate final stats
            self.import_stats['processing_time'] = time.time() - start_time
            self.import_stats['total_errors'] = len(self.import_stats['files_with_errors'])
            
            # Determine overall success
            overall_success = (
                self.import_stats['files_processed'] > 0 and 
                self.import_stats['total_errors'] == 0
            )
            
            result_message = f"Processed {self.import_stats['files_processed']}/{len(new_files)} files successfully"
            if self.import_stats['total_errors'] > 0:
                result_message += f" with {self.import_stats['total_errors']} errors"
            
            return {
                'success': overall_success,
                'message': result_message,
                'files_discovered': len(new_files),
                'files_processed': self.import_stats['files_processed'],
                'records_imported': self.import_stats['total_records_imported'],
                'stats': self.import_stats
            }
        
        except Exception as e:
            error_msg = f"Critical error in discover_and_import: {str(e)}"
            logger.error(error_msg)
            self.import_stats['processing_time'] = time.time() - start_time
            return {
                'success': False,
                'message': error_msg,
                'stats': self.import_stats
            }
    
    def _process_single_file(self, 
                            file_path: Path, 
                            db_ops: DatabaseOperations, 
                            table_schema: List[Dict], 
                            batch_size: int) -> Dict:
        """
        Process a single Excel file for import.
        
        Args:
            file_path: Path to the Excel file
            db_ops: Database operations instance
            table_schema: Database table schema
            batch_size: Batch size for database operations
            
        Returns:
            Dictionary with processing results
        """
        try:
            # Process the Excel file
            processor = ExcelProcessor(file_path)
            excel_data = processor.read_sheet()
            
            if excel_data.empty:
                return {
                    'success': False,
                    'error': 'Excel file is empty',
                    'records_imported': 0
                }
            
            logger.info(f"Read {len(excel_data)} rows from {file_path.name}")
            
            # Convert data for database import
            converted_records = []
            conversion_errors = 0
            
            for row_num, (_, row) in enumerate(excel_data.iterrows(), 1):
                try:
                    record_dict = row.to_dict()
                    
                    # Apply data type conversion
                    record_dict = db_ops.convert_data_for_access(record_dict, table_schema)
                    
                    if record_dict:
                        converted_records.append(record_dict)
                
                except Exception as e:
                    conversion_errors += 1
                    if conversion_errors <= 5:  # Log first 5 errors
                        logger.warning(f"Row {row_num} conversion error: {e}")
            
            if not converted_records:
                return {
                    'success': False,
                    'error': f'No valid records after conversion (conversion errors: {conversion_errors})',
                    'records_imported': 0
                }
            
            # Import to database using batch method
            imported_count = db_ops.insert_records_batch(
                self.target_table, converted_records, batch_size=batch_size
            )
            
            logger.info(f"Imported {imported_count} records from {file_path.name}")
            
            if conversion_errors > 0:
                logger.warning(f"File {file_path.name} had {conversion_errors} conversion errors")
            
            return {
                'success': True,
                'records_imported': imported_count,
                'conversion_errors': conversion_errors
            }
        
        except Exception as e:
            error_msg = f"Error processing file {file_path.name}: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'records_imported': 0
            }
    
    def force_import_file(self, file_path: str, move_after: bool = True) -> Dict:
        """
        Force import a specific file (for manual operation).
        
        Args:
            file_path: Path to the file to import
            move_after: Whether to move the file after processing
            
        Returns:
            Dictionary with import results
        """
        import time
        start_time = time.time()
        
        try:
            file_obj = Path(file_path)
            
            if not file_obj.exists():
                return {
                    'success': False,
                    'message': f'File not found: {file_path}'
                }
            
            # Connect to database
            db_ops = DatabaseOperations(self.database_path)
            if not db_ops.connect():
                return {
                    'success': False,
                    'message': 'Failed to connect to database'
                }
            
            try:
                # Get table schema
                table_schema = db_ops.get_table_schema(self.target_table)
                
                # Process the file
                result = self._process_single_file(file_obj, db_ops, table_schema, 1000)
                
                if result['success'] and move_after:
                    moved_path = self.file_discovery.move_processed_file(file_obj)
                    if moved_path:
                        result['moved_to'] = str(moved_path)
                
                result['processing_time'] = time.time() - start_time
                return result
            
            finally:
                db_ops.close()
        
        except Exception as e:
            return {
                'success': False,
                'message': f'Error in force import: {str(e)}',
                'processing_time': time.time() - start_time
            }
