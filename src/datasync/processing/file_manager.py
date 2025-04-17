"""
File management module for handling file operations.
"""

import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class FileManager:
    """Handles file system operations."""
    
    def file_exists(self, file_path: Path) -> bool:
        """Check if a file exists."""
        return file_path.exists()
    
    def get_file_size(self, file_path: Path) -> int:
        """Get the size of a file in bytes."""
        return file_path.stat().st_size
    
    def create_directory(self, directory_path: Path) -> None:
        """Create a directory if it doesn't exist."""
        directory_path.mkdir(parents=True, exist_ok=True)
    
    def delete_file(self, file_path: Path) -> None:
        """Delete a file if it exists."""
        if file_path.exists():
            file_path.unlink()
    
    def list_files(self, directory_path: Path, pattern: str = "*") -> list[Path]:
        """List files in a directory matching a pattern."""
        return list(directory_path.glob(pattern)) 