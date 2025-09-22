"""
File discovery and management utility for DataSync.
Handles automatic discovery of new Excel files and moving processed files.
"""

import os
import shutil
from pathlib import Path
from typing import List, Optional
import glob
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class FileDiscovery:
    """Manages automatic file discovery and organization for DataSync."""
    
    def __init__(self, data_dir: str = "data", loaded_dir: str = "data/loaded"):
        """
        Initialize FileDiscovery with data directories.
        
        Args:
            data_dir: Directory where new files are placed
            loaded_dir: Directory where processed files are moved
        """
        self.data_dir = Path(data_dir)
        self.loaded_dir = Path(loaded_dir)
        
        # Ensure directories exist
        self.data_dir.mkdir(exist_ok=True)
        self.loaded_dir.mkdir(exist_ok=True)
        
        # Supported Excel file extensions
        self.excel_extensions = ['.xlsx', '.xls']
        
    def discover_new_files(self) -> List[Path]:
        """
        Discover new Excel files in the data directory.
        
        Returns:
            List of Path objects for new Excel files to process
        """
        new_files = []
        
        try:
            # Search for Excel files in data directory
            for extension in self.excel_extensions:
                pattern = str(self.data_dir / f"*{extension}")
                found_files = glob.glob(pattern)
                
                for file_path in found_files:
                    file_obj = Path(file_path)
                    # Skip files that are already in subdirectories
                    if file_obj.parent == self.data_dir:
                        new_files.append(file_obj)
            
            # Sort files by modification time (oldest first)
            new_files.sort(key=lambda x: x.stat().st_mtime)
            
            logger.info(f"Discovered {len(new_files)} new Excel files: {[f.name for f in new_files]}")
            return new_files
            
        except Exception as e:
            logger.error(f"Error discovering files: {e}")
            return []
    
    def move_processed_file(self, file_path: Path, add_timestamp: bool = True) -> Optional[Path]:
        """
        Move a processed file to the loaded directory.
        
        Args:
            file_path: Path to the file to move
            add_timestamp: Whether to add timestamp to filename
            
        Returns:
            Path to the moved file, or None if failed
        """
        try:
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                return None
            
            # Prepare destination filename
            if add_timestamp:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                stem = file_path.stem
                suffix = file_path.suffix
                new_filename = f"{stem}_{timestamp}{suffix}"
            else:
                new_filename = file_path.name
            
            destination = self.loaded_dir / new_filename
            
            # Handle filename conflicts
            counter = 1
            original_dest = destination
            while destination.exists():
                stem = original_dest.stem
                suffix = original_dest.suffix
                destination = self.loaded_dir / f"{stem}_{counter:03d}{suffix}"
                counter += 1
            
            # Move the file
            shutil.move(str(file_path), str(destination))
            logger.info(f"Moved processed file: {file_path.name} -> {destination.name}")
            
            return destination
            
        except Exception as e:
            logger.error(f"Error moving file {file_path}: {e}")
            return None
    
    def get_data_directory_status(self) -> dict:
        """
        Get status information about the data directories.
        
        Returns:
            Dictionary with directory status information
        """
        try:
            new_files = self.discover_new_files()
            
            # Count files in loaded directory
            loaded_files = []
            for extension in self.excel_extensions:
                pattern = str(self.loaded_dir / f"*{extension}")
                loaded_files.extend(glob.glob(pattern))
            
            status = {
                'data_dir': str(self.data_dir),
                'loaded_dir': str(self.loaded_dir),
                'new_files_count': len(new_files),
                'new_files': [f.name for f in new_files],
                'loaded_files_count': len(loaded_files),
                'has_new_files': len(new_files) > 0,
                'directories_exist': self.data_dir.exists() and self.loaded_dir.exists()
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting directory status: {e}")
            return {
                'error': str(e),
                'has_new_files': False,
                'directories_exist': False
            }
    
    def clean_empty_data_directory(self) -> bool:
        """
        Clean up the data directory, leaving only the loaded subdirectory.
        
        Returns:
            True if cleanup was successful
        """
        try:
            # Get list of items in data directory
            items = list(self.data_dir.iterdir())
            
            # Remove all items except the loaded directory
            for item in items:
                if item.name != "loaded":
                    if item.is_file():
                        item.unlink()
                        logger.info(f"Removed file: {item.name}")
                    elif item.is_dir():
                        shutil.rmtree(item)
                        logger.info(f"Removed directory: {item.name}")
            
            logger.info("Data directory cleaned successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error cleaning data directory: {e}")
            return False
    
    def validate_file_for_processing(self, file_path: Path) -> bool:
        """
        Validate that a file is ready for processing.
        
        Args:
            file_path: Path to the file to validate
            
        Returns:
            True if file is valid for processing
        """
        try:
            # Check if file exists
            if not file_path.exists():
                logger.error(f"File does not exist: {file_path}")
                return False
            
            # Check if it's an Excel file
            if file_path.suffix.lower() not in self.excel_extensions:
                logger.error(f"Not an Excel file: {file_path}")
                return False
            
            # Check if file is not empty
            if file_path.stat().st_size == 0:
                logger.error(f"File is empty: {file_path}")
                return False
            
            # Try to access the file (check if it's not locked)
            try:
                with open(file_path, 'rb') as f:
                    f.read(1)
            except PermissionError:
                logger.error(f"File is locked or permission denied: {file_path}")
                return False
            
            logger.info(f"File validated successfully: {file_path.name}")
            return True
            
        except Exception as e:
            logger.error(f"Error validating file {file_path}: {e}")
            return False
