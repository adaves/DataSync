"""
Unit tests for file management functionality.
"""

import pytest
import os
import sys
from pathlib import Path
import tempfile
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datasync.processing.file_manager import FileManager

@pytest.fixture
def file_manager():
    """Create a FileManager instance for testing."""
    with FileManager() as fm:
        yield fm

@pytest.fixture
def test_directory():
    """Create a temporary directory with test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test files and directories
        temp_path = Path(temp_dir)
        (temp_path / "test.txt").write_text("test content")
        (temp_path / "test.bin").write_bytes(b"binary content")
        (temp_path / "subdir").mkdir()
        (temp_path / "subdir" / "nested.txt").write_text("nested content")
        yield temp_path

def test_context_manager(test_directory):
    """Test FileManager context manager functionality."""
    test_file = test_directory / "context_test.txt"
    
    with FileManager() as fm:
        fm.write_text(test_file, "test")
        assert test_file.exists()
        assert len(fm._locks) == 0  # Locks should be released after write

@pytest.mark.skipif(sys.platform == 'win32', reason="File locking behaves differently on Windows")
def test_file_locking(test_directory):
    """Test file locking mechanism."""
    test_file = test_directory / "lock_test.txt"
    test_file.write_text("initial")
    
    def write_with_lock(fm, content):
        with fm.file_lock(test_file):
            time.sleep(0.1)  # Simulate work
            test_file.write_text(content)
    
    with FileManager() as fm1, FileManager() as fm2:
        t1 = threading.Thread(target=write_with_lock, args=(fm1, "content1"))
        t2 = threading.Thread(target=write_with_lock, args=(fm2, "content2"))
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        # Content should be either "content1" or "content2", not mixed
        content = test_file.read_text()
        assert content in ["content1", "content2"]

def test_file_exists(file_manager, test_directory):
    """Test file existence checking."""
    assert file_manager.file_exists(test_directory / "test.txt")
    assert not file_manager.file_exists(test_directory / "nonexistent.txt")

def test_get_file_size(file_manager, test_directory):
    """Test file size retrieval."""
    assert file_manager.get_file_size(test_directory / "test.txt") == len("test content")
    with pytest.raises(FileNotFoundError):
        file_manager.get_file_size(test_directory / "nonexistent.txt")

def test_create_directory(file_manager, test_directory):
    """Test directory creation."""
    new_dir = test_directory / "new" / "nested"
    file_manager.create_directory(new_dir)
    assert new_dir.is_dir()
    
    # Creating existing directory should not raise error
    file_manager.create_directory(new_dir)

def test_delete_file(file_manager, test_directory):
    """Test file deletion."""
    test_file = test_directory / "to_delete.txt"
    test_file.write_text("delete me")
    
    file_manager.delete_file(test_file)
    assert not test_file.exists()
    
    # Deleting non-existent file should not raise error
    file_manager.delete_file(test_file)
    
    # Attempting to delete directory as file should raise error
    with pytest.raises(IsADirectoryError):
        file_manager.delete_file(test_directory / "subdir")

def test_list_files(file_manager, test_directory):
    """Test file listing functionality."""
    files = file_manager.list_files(test_directory)
    assert len(files) == 3  # test.txt, test.bin, and subdir
    
    txt_files = file_manager.list_files(test_directory, "*.txt")
    assert len(txt_files) == 1
    assert txt_files[0].name == "test.txt"
    
    with pytest.raises(FileNotFoundError):
        file_manager.list_files(test_directory / "nonexistent")

def test_text_operations(file_manager, test_directory):
    """Test text file read/write operations."""
    test_file = test_directory / "text_ops.txt"
    
    # Test write_text and read_text with default encoding
    content = "Hello, 世界!"
    file_manager.write_text(test_file, content)
    assert file_manager.read_text(test_file) == content
    
    # Test with explicit encoding
    file_manager.write_text(test_file, content, encoding="utf-16")
    assert file_manager.read_text(test_file, encoding="utf-16") == content

def test_stream_operations(file_manager, test_directory):
    """Test streaming file operations."""
    test_file = test_directory / "stream_test.bin"
    
    # Create test data
    data = [bytes([i % 256]) * 1024 for i in range(10)]  # 10KB of test data
    
    # Test stream_write
    file_manager.stream_write(test_file, iter(data))
    
    # Test stream_read
    chunks = list(file_manager.stream_read(test_file, chunk_size=1024))
    assert len(chunks) == 10
    assert all(len(chunk) == 1024 for chunk in chunks)
    assert b"".join(chunks) == b"".join(data)

@pytest.mark.skipif(sys.platform == 'win32', reason="Concurrent file access behaves differently on Windows")
def test_concurrent_access(file_manager, test_directory):
    """Test concurrent file access."""
    test_file = test_directory / "concurrent.txt"
    iterations = 100
    
    def write_number(n):
        file_manager.write_text(test_file, f"{n}\n", encoding="utf-8")
        time.sleep(0.001)  # Simulate work
        return n
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(write_number, range(iterations)))
    
    # Verify all operations completed
    assert len(results) == iterations
    
    # Verify file content is not corrupted
    content = file_manager.read_text(test_file)
    assert len(content.strip().split("\n")) == 1  # Should only contain one number

def test_error_handling(file_manager, test_directory):
    """Test error handling scenarios."""
    non_existent = test_directory / "nonexistent.txt"
    readonly_file = test_directory / "readonly.txt"
    readonly_file.write_text("readonly")
    
    # Test operations on non-existent file
    with pytest.raises(FileNotFoundError):
        file_manager.read_text(non_existent)
    
    # Test operations on read-only file
    if sys.platform != 'win32':  # Skip on Windows as chmod behaves differently
        os.chmod(readonly_file, 0o444)  # Make file read-only
        with pytest.raises(OSError):
            file_manager.write_text(readonly_file, "try to write")
        os.chmod(readonly_file, 0o644)  # Restore permissions

def test_path_handling(file_manager, test_directory):
    """Test path handling with different input types."""
    # Test with string paths
    str_path = str(test_directory / "string_path.txt")
    file_manager.write_text(str_path, "test")
    assert file_manager.file_exists(str_path)
    
    # Test with Path objects
    path_obj = Path(test_directory) / "path_obj.txt"
    file_manager.write_text(path_obj, "test")
    assert file_manager.file_exists(path_obj)
    
    # Test with relative paths
    rel_path = "./relative_path.txt"
    try:
        file_manager.write_text(rel_path, "test")
        assert file_manager.file_exists(rel_path)
    finally:
        if Path(rel_path).exists():
            Path(rel_path).unlink()

def test_cleanup(file_manager, test_directory):
    """Test cleanup of resources."""
    test_file = test_directory / "cleanup_test.txt"
    
    # Create some locks
    with file_manager.file_lock(test_file):
        assert len(file_manager._locks) == 1
        # Let the context manager handle cleanup
    
    assert len(file_manager._locks) == 0  # Verify locks are released 