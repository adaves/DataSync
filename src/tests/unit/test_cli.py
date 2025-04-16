"""
Unit tests for the CLI implementation.
"""

import pytest
from click.testing import CliRunner
from datasync.cli import cli
import os
import tempfile
from pathlib import Path

@pytest.fixture
def runner():
    return CliRunner()

@pytest.fixture
def temp_db():
    """Create a temporary Access database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.accdb', delete=False) as tmp:
        return tmp.name

@pytest.fixture
def temp_excel():
    """Create a temporary Excel file for testing."""
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        return tmp.name

def test_cli_help(runner):
    """Test that the CLI help message is displayed correctly."""
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert "DataSync - A tool for synchronizing data" in result.output

def test_sync_command(runner, temp_db, temp_excel):
    """Test the sync command with various options."""
    # Test with invalid source
    result = runner.invoke(cli, ['sync', 'nonexistent.accdb', temp_excel])
    assert result.exit_code != 0
    assert "Error" in result.output

    # Test with valid source and destination
    result = runner.invoke(cli, ['sync', temp_db, temp_excel])
    assert result.exit_code == 0
    assert "Synchronization completed" in result.output

    # Test with batch size option
    result = runner.invoke(cli, ['sync', temp_db, temp_excel, '--batch-size', '500'])
    assert result.exit_code == 0
    assert "Processing batch" in result.output

def test_validate_command(runner, temp_db):
    """Test the validate command with various options."""
    # Test with invalid database
    result = runner.invoke(cli, ['validate', 'nonexistent.accdb'])
    assert result.exit_code != 0
    assert "Error" in result.output

    # Test with valid database
    result = runner.invoke(cli, ['validate', temp_db])
    assert result.exit_code == 0
    assert "Validation completed" in result.output

    # Test with specific table
    result = runner.invoke(cli, ['validate', temp_db, '--table', 'test_table'])
    assert result.exit_code == 0

    # Test with output file
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
        result = runner.invoke(cli, ['validate', temp_db, '--output', tmp.name])
        assert result.exit_code == 0
        assert os.path.exists(tmp.name)

def test_monitor_command(runner, temp_db):
    """Test the monitor command with various options."""
    # Test with invalid database
    result = runner.invoke(cli, ['monitor', 'nonexistent.accdb'])
    assert result.exit_code != 0
    assert "Error" in result.output

    # Test with valid database and short duration
    result = runner.invoke(cli, ['monitor', temp_db, '--duration', '1'])
    assert result.exit_code == 0
    assert "Monitoring completed" in result.output

    # Test with output file
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
        result = runner.invoke(cli, ['monitor', temp_db, '--duration', '1', '--output', tmp.name])
        assert result.exit_code == 0
        assert os.path.exists(tmp.name)

def test_invalid_destination_type(runner, temp_db):
    """Test handling of invalid destination file types."""
    result = runner.invoke(cli, ['sync', temp_db, 'invalid.txt'])
    assert result.exit_code != 0
    assert "Unsupported destination file type" in result.output

def test_cleanup(temp_db, temp_excel):
    """Clean up temporary files after tests."""
    for file in [temp_db, temp_excel]:
        if os.path.exists(file):
            os.unlink(file) 