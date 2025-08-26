"""
File management functionality for the DataSync application.
"""

import os
import sys
import time
from pathlib import Path
import logging
import io
from typing import List, Union, Optional, Iterator
from contextlib import contextmanager

if sys.platform == 'win32':
    import msvcrt
else:
    import fcntl

logger = logging.getLogger(__name__)

class FileManager:
    """Manages file operations with proper locking and error handling."""
    
    def __init__(self, default_encoding: str = 'utf-8'):
        """Initialize FileManager with default encoding."""
        self.default_encoding = default_encoding
        self._locks = {}  # For backward compatibility with tests
        self._file_handles = {}
        self._max_retries = 3
        self._retry_delay = 0.1  # seconds
    
    def __enter__(self):
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and cleanup resources."""
        self.release_all_locks()
    
    @contextmanager
    def file_lock(self, file_path: Union[str, Path]):
        """Context manager for file locking."""
        path = Path(file_path)
        if sys.platform == 'win32':
            # Windows file locking
            try:
                handle = self._get_windows_handle(path)
                self._file_handles[path] = handle
                self._locks[path] = handle  # For backward compatibility
                yield
            finally:
                self.release_lock(path)
        else:
            # Unix file locking
            with open(path, 'a') as f:
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    yield
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    
    def _get_windows_handle(self, path: Path) -> int:
        """Get Windows file handle with retry logic."""
        for attempt in range(self._max_retries):
            try:
                handle = os.open(str(path), os.O_RDWR | os.O_CREAT | os.O_BINARY)
                return handle
            except PermissionError:
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay)
                    continue
                raise
    
    def release_lock(self, file_path: Union[str, Path]) -> None:
        """Release lock on a specific file."""
        path = Path(file_path)
        if path in self._file_handles:
            try:
                os.close(self._file_handles[path])
            except (OSError, KeyError):
                pass
            finally:
                del self._file_handles[path]
                if path in self._locks:  # For backward compatibility
                    del self._locks[path]
    
    def release_all_locks(self) -> None:
        """Release all file locks."""
        for path in list(self._file_handles.keys()):
            self.release_lock(path)
    
    def write_text(self, file_path: Union[str, Path], content: str, encoding: Optional[str] = None) -> None:
        """Write text to a file with proper locking."""
        path = Path(file_path)
        for attempt in range(self._max_retries):
            try:
                with self.file_lock(path):
                    with open(str(path), 'w', encoding=encoding or self.default_encoding) as f:
                        f.write(content)
                break
            except PermissionError:
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay)
                    continue
                raise
    
    def read_text(self, file_path: Union[str, Path], encoding: Optional[str] = None) -> str:
        """Read text from a file with proper locking."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        for attempt in range(self._max_retries):
            try:
                with self.file_lock(path):
                    with open(str(path), 'r', encoding=encoding or self.default_encoding) as f:
                        return f.read()
            except PermissionError:
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay)
                    continue
                raise
    
    def stream_write(self, file_path: Union[str, Path], data_iterator: Iterator[bytes]) -> None:
        """Stream write data to a file."""
        path = Path(file_path)
        for attempt in range(self._max_retries):
            try:
                with self.file_lock(path):
                    with open(str(path), 'wb') as f:
                        for chunk in data_iterator:
                            f.write(chunk)
                break
            except PermissionError:
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay)
                    continue
                raise
    
    def stream_read(self, file_path: Union[str, Path], chunk_size: int = 8192) -> Iterator[bytes]:
        """Stream read data from a file."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
            
        for attempt in range(self._max_retries):
            try:
                with self.file_lock(path):
                    with open(str(path), 'rb') as f:
                        while True:
                            chunk = f.read(chunk_size)
                            if not chunk:
                                break
                            yield chunk
                break
            except PermissionError:
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay)
                    continue
                raise
    
    def file_exists(self, file_path: Union[str, Path]) -> bool:
        """Check if a file exists."""
        return Path(file_path).exists()
    
    def get_file_size(self, file_path: Union[str, Path]) -> int:
        """Get file size in bytes."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        return path.stat().st_size
    
    def create_directory(self, directory_path: Union[str, Path]) -> None:
        """Create a directory if it doesn't exist."""
        Path(directory_path).mkdir(parents=True, exist_ok=True)
    
    def delete_file(self, file_path: Union[str, Path]) -> None:
        """Delete a file with retry logic."""
        path = Path(file_path)
        if not path.exists():
            return  # Silently ignore non-existent files
            
        if path.is_dir():
            raise IsADirectoryError(f"Cannot delete directory as file: {path}")
            
        for attempt in range(self._max_retries):
            try:
                path.unlink()
                break
            except PermissionError:
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay)
                    continue
                raise
    
    def list_files(self, directory_path: Union[str, Path], pattern: str = "*") -> List[Path]:
        """List files in a directory matching a pattern."""
        path = Path(directory_path)
        if not path.exists():
            raise FileNotFoundError(f"Directory not found: {path}")
        return list(path.glob(pattern)) 