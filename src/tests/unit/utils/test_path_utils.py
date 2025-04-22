"""
Tests for path utility functions.
"""
import pytest
from pathlib import Path
from datasync.utils.path_utils import (
    normalize_path,
    path_to_connection_string,
    ensure_directory_exists,
    is_valid_database_path,
    get_relative_path,
    join_paths,
    get_file_extension
)

def test_normalize_path(tmp_path):
    """Test path normalization."""
    # Test with string path
    path_str = str(tmp_path / "test.txt")
    normalized = normalize_path(path_str)
    assert isinstance(normalized, Path)
    assert normalized.is_absolute()
    
    # Test with Path object
    path_obj = tmp_path / "test.txt"
    normalized = normalize_path(path_obj)
    assert isinstance(normalized, Path)
    assert normalized.is_absolute()
    
    # Test with relative path
    normalized = normalize_path("test.txt")
    assert isinstance(normalized, Path)
    assert normalized.is_absolute()

def test_path_to_connection_string(tmp_path):
    """Test path conversion for connection strings."""
    test_path = tmp_path / "test.accdb"
    conn_str_path = path_to_connection_string(test_path)
    
    # Should have double backslashes
    assert "\\\\" in conn_str_path
    # Should be absolute path
    assert str(test_path.absolute()).replace("\\", "\\\\") == conn_str_path

def test_ensure_directory_exists(tmp_path):
    """Test directory creation."""
    # Test single level directory
    test_dir = tmp_path / "test_dir"
    result = ensure_directory_exists(test_dir)
    assert result.exists()
    assert result.is_dir()
    
    # Test nested directories
    nested_dir = tmp_path / "parent" / "child" / "grandchild"
    result = ensure_directory_exists(nested_dir)
    assert result.exists()
    assert result.is_dir()
    
    # Test with existing directory
    result = ensure_directory_exists(test_dir)
    assert result.exists()
    assert result.is_dir()

def test_is_valid_database_path(tmp_path):
    """Test database path validation."""
    # Test with non-existent file
    assert not is_valid_database_path(tmp_path / "nonexistent.accdb")
    
    # Test with wrong extension
    wrong_ext = tmp_path / "test.txt"
    wrong_ext.touch()
    assert not is_valid_database_path(wrong_ext)
    
    # Test with valid extension
    valid_db = tmp_path / "test.accdb"
    valid_db.touch()
    assert is_valid_database_path(valid_db)
    
    # Test with directory
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    assert not is_valid_database_path(test_dir)

def test_get_relative_path(tmp_path):
    """Test relative path conversion."""
    base = tmp_path / "base"
    base.mkdir()
    target = base / "subdir" / "file.txt"
    target.parent.mkdir(parents=True)
    target.touch()
    
    # Test with explicit base path
    rel_path = get_relative_path(target, base)
    expected = Path("subdir/file.txt")
    assert rel_path == expected
    
    # Test with default base path (cwd)
    with pytest.raises(ValueError):  # Should raise if path not relative to cwd
        get_relative_path(target)

def test_join_paths(tmp_path):
    """Test path joining."""
    # Test with string paths
    result = join_paths("dir1", "dir2", "file.txt")
    assert isinstance(result, Path)
    assert str(result) == str(Path("dir1") / "dir2" / "file.txt")
    
    # Test with Path objects
    p1 = Path("dir1")
    p2 = Path("dir2")
    result = join_paths(p1, p2, "file.txt")
    assert isinstance(result, Path)
    assert str(result) == str(Path("dir1") / "dir2" / "file.txt")
    
    # Test with mixed types
    result = join_paths(tmp_path, Path("dir1"), "file.txt")
    assert isinstance(result, Path)
    assert str(result) == str(tmp_path / "dir1" / "file.txt")

def test_get_file_extension():
    """Test file extension extraction."""
    # Test with string path
    assert get_file_extension("test.txt") == ".txt"
    assert get_file_extension("test.TXT") == ".txt"  # Should be lowercase
    assert get_file_extension("test") == ""  # No extension
    
    # Test with Path object
    assert get_file_extension(Path("test.accdb")) == ".accdb"
    assert get_file_extension(Path("test.ACCDB")) == ".accdb"  # Should be lowercase
    assert get_file_extension(Path("test")) == ""  # No extension
    
    # Test with multiple dots
    assert get_file_extension("test.tar.gz") == ".gz" 