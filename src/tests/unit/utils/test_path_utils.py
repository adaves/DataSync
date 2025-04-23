"""
Tests for path utility functions.
"""
import pytest
from pathlib import Path
import os
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
    """Test path normalization with various edge cases."""
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
    
    # Test with empty string
    with pytest.raises(ValueError):
        normalize_path("")
    
    # Test with None
    with pytest.raises(TypeError):
        normalize_path(None)
    
    # Test with special characters
    special_path = tmp_path / "test!@#$%^&().txt"
    normalized = normalize_path(special_path)
    assert isinstance(normalized, Path)
    assert normalized.name == "test!@#$%^&().txt"
    
    # Test with mixed slashes
    mixed_path = str(tmp_path / "dir1/dir2\\dir3/file.txt").replace("/", os.sep)
    normalized = normalize_path(mixed_path)
    assert isinstance(normalized, Path)
    assert normalized.name == "file.txt"

def test_path_to_connection_string(tmp_path):
    """Test path conversion for connection strings with edge cases."""
    # Create test database file
    test_path = tmp_path / "test.accdb"
    test_path.touch()
    
    # Basic test
    conn_str_path = path_to_connection_string(test_path)
    assert "\\\\" in conn_str_path
    assert str(test_path.absolute()).replace("\\", "\\\\") == conn_str_path
    
    # Test with spaces in path
    space_path = tmp_path / "test folder" / "my database.accdb"
    space_path.parent.mkdir(parents=True, exist_ok=True)
    space_path.touch()
    conn_str = path_to_connection_string(space_path)
    assert "test folder" in conn_str
    assert "my database.accdb" in conn_str
    
    # Test with special characters
    special_path = tmp_path / "test!@#$" / "db.accdb"
    special_path.parent.mkdir(parents=True, exist_ok=True)
    special_path.touch()
    conn_str = path_to_connection_string(special_path)
    assert "test!@#$" in conn_str
    
    # Test with non-Access file
    with pytest.raises(ValueError):
        path_to_connection_string(tmp_path / "test.txt")
    
    # Test with None
    with pytest.raises(TypeError):
        path_to_connection_string(None)

def test_ensure_directory_exists(tmp_path):
    """Test directory creation with edge cases."""
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
    
    # Test with path that exists as a file
    test_file = tmp_path / "test_file"
    test_file.touch()
    with pytest.raises(FileExistsError):
        ensure_directory_exists(test_file)
    
    # Test with very long path
    if os.name == 'nt':
        # On Windows, create a path that exceeds MAX_PATH (260 characters)
        # Calculate remaining length for the directory name
        max_path = 260
        current_length = len(str(tmp_path))
        remaining_length = max_path - current_length - 2  # Account for path separator and null terminator
        if remaining_length > 0:
            long_name = "x" * remaining_length
            long_path = tmp_path / long_name
            result = ensure_directory_exists(long_path)
            assert result.exists()
            assert result.is_dir()
    else:
        # On other systems, use a reasonable long name
        long_name = "x" * 100
        long_path = tmp_path / long_name
        result = ensure_directory_exists(long_path)
        assert result.exists()
        assert result.is_dir()

def test_is_valid_database_path(tmp_path):
    """Test database path validation with edge cases."""
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
    
    # Test with case-insensitive extensions
    upper_ext = tmp_path / "test.ACCDB"
    upper_ext.touch()
    assert is_valid_database_path(upper_ext)
    
    # Test with multiple extensions
    multi_ext = tmp_path / "test.backup.accdb"
    multi_ext.touch()
    assert is_valid_database_path(multi_ext)
    
    # Test with None
    with pytest.raises(TypeError):
        is_valid_database_path(None)

def test_get_relative_path(tmp_path):
    """Test relative path conversion with edge cases."""
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
    
    # Test with . and .. in paths
    dot_path = base / "." / "subdir" / "file.txt"
    rel_path = get_relative_path(dot_path, base)
    assert rel_path == Path("subdir/file.txt")
    
    parent_path = base / "subdir" / ".." / "file.txt"
    rel_path = get_relative_path(parent_path, base)
    assert rel_path == Path("file.txt")
    
    # Test with case sensitivity
    if os.name == 'nt':  # Windows
        mixed_case = base / "SubDir" / "File.txt"
        mixed_case.parent.mkdir(parents=True, exist_ok=True)
        mixed_case.touch()
        rel_path = get_relative_path(mixed_case, base)
        assert rel_path.as_posix() == "SubDir/File.txt"
    
    # Test with None
    with pytest.raises(TypeError):
        get_relative_path(None)
    with pytest.raises(TypeError):
        get_relative_path(target, None)

def test_join_paths(tmp_path):
    """Test path joining with edge cases."""
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
    
    # Test with empty strings
    result = join_paths("dir1", "", "file.txt")
    assert str(result) == str(Path("dir1") / "file.txt")
    
    # Test with None values
    with pytest.raises(TypeError):
        join_paths(None)
    with pytest.raises(TypeError):
        join_paths("dir1", None, "file.txt")
    
    # Test with absolute paths
    abs_path = os.path.abspath("dir1")
    result = join_paths(abs_path, "dir2", "file.txt")
    assert result.is_absolute()
    assert str(result) == str(Path(abs_path) / "dir2" / "file.txt")

def test_get_file_extension():
    """Test file extension extraction with edge cases."""
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
    
    # Test with leading dot
    assert get_file_extension(".gitignore") == ""
    
    # Test with multiple extensions
    assert get_file_extension("archive.tar.gz") == ".gz"
    assert get_file_extension("backup.2023.accdb") == ".accdb"
    
    # Test with None
    with pytest.raises(TypeError):
        get_file_extension(None)
    
    # Test with empty string
    assert get_file_extension("") == "" 