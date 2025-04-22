"""
Unit tests for the CLI implementation.
"""

import pytest
from click.testing import CliRunner
from pathlib import Path
import os
from datasync.cli import cli, get_destination_type
import tempfile
import pandas as pd
from unittest.mock import patch, MagicMock
import click

@pytest.fixture
def runner():
    return CliRunner()

@pytest.fixture
def mock_db_ops(mocker):
    return mocker.patch('datasync.cli.DatabaseOperations')

@pytest.fixture
def temp_db(tmp_path):
    db_file = tmp_path / "test.accdb"
    db_file.touch()
    return str(db_file)

@pytest.fixture
def temp_excel(tmp_path):
    excel_file = tmp_path / "test.xlsx"
    excel_file.touch()
    return str(excel_file)

# Basic Command Execution Tests
def test_cli_help(runner):
    """Test that the CLI help message is displayed correctly."""
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert 'Synchronize data between Access and Excel' in result.output

def test_cli_version(runner):
    """Test that the CLI version is displayed correctly."""
    result = runner.invoke(cli, ['--version'])
    assert result.exit_code == 0
    assert 'version' in result.output.lower()

# Command Argument Parsing Tests
def test_sync_command_argument_parsing(runner, mock_db_ops, temp_db, temp_excel):
    """Test sync command argument parsing."""
    # Test required arguments
    result = runner.invoke(cli, ['sync'])
    assert result.exit_code == 2
    assert 'Missing argument' in result.output

    # Test optional arguments
    result = runner.invoke(cli, ['sync', temp_db, temp_excel, '--batch-size', '500'])
    assert result.exit_code == 0
    assert 'Successfully synchronized data' in result.output

    # Test invalid batch size
    result = runner.invoke(cli, ['sync', temp_db, temp_excel, '--batch-size', '0'])
    assert result.exit_code == 2
    assert 'Batch size must be greater than 0' in result.output

    # Test non-existent source
    result = runner.invoke(cli, ['sync', 'nonexistent.accdb', temp_excel])
    assert result.exit_code == 2
    assert "Invalid value for 'SOURCE': Path 'nonexistent.accdb' does not exist" in result.output

def test_validate_command_argument_parsing(runner, mock_db_ops, temp_db):
    """Test validate command argument parsing."""
    # Test required arguments
    result = runner.invoke(cli, ['validate'])
    assert result.exit_code == 2
    assert 'Missing argument' in result.output

    # Test optional arguments
    result = runner.invoke(cli, ['validate', temp_db, '--table', 'test_table', '--year', '2024'])
    assert result.exit_code == 0
    assert 'Database is valid' in result.output

    # Test non-existent database
    result = runner.invoke(cli, ['validate', 'nonexistent.accdb'])
    assert result.exit_code == 2
    assert "Invalid value for 'DATABASE': Path 'nonexistent.accdb' does not exist" in result.output

def test_monitor_command_argument_parsing(runner, mock_db_ops, temp_db):
    """Test monitor command argument parsing."""
    # Test required arguments
    result = runner.invoke(cli, ['monitor'])
    assert result.exit_code == 2
    assert 'Missing argument' in result.output

    # Test optional arguments
    result = runner.invoke(cli, ['monitor', temp_db, '--interval', '30', '--duration', '60'])
    assert result.exit_code == 0

    # Test invalid interval
    result = runner.invoke(cli, ['monitor', temp_db, '--interval', '0'])
    assert result.exit_code == 2
    assert 'Interval must be greater than 0' in result.output

    # Test non-existent database
    result = runner.invoke(cli, ['monitor', 'nonexistent.accdb'])
    assert result.exit_code == 2
    assert "Invalid value for 'DATABASE': Path 'nonexistent.accdb' does not exist" in result.output

# Command Error Handling Tests
def test_sync_command_error_handling(runner, mock_db_ops, temp_db, temp_excel):
    """Test sync command error handling."""
    # Setup mock
    instance = mock_db_ops.return_value
    instance.sync_to_excel.side_effect = Exception("Connection failed")
    
    # Test non-existent source
    result = runner.invoke(cli, ['sync', 'nonexistent.accdb', temp_excel])
    assert result.exit_code == 2
    assert "Invalid value for 'SOURCE': Path 'nonexistent.accdb' does not exist" in result.output

    # Test runtime error
    result = runner.invoke(cli, ['sync', temp_db, temp_excel])
    assert result.exit_code == 1
    assert 'Error: Connection failed' in result.output

def test_validate_command_error_handling(runner, mock_db_ops, temp_db):
    """Test validate command error handling."""
    # Setup mock
    instance = mock_db_ops.return_value
    instance.validate_database.side_effect = Exception("Validation failed")
    
    # Test non-existent database
    result = runner.invoke(cli, ['validate', 'nonexistent.accdb'])
    assert result.exit_code == 2
    assert "Invalid value for 'DATABASE': Path 'nonexistent.accdb' does not exist" in result.output

    # Test runtime error
    result = runner.invoke(cli, ['validate', temp_db])
    assert result.exit_code == 1
    assert 'Error: Validation failed' in result.output

def test_monitor_command_error_handling(runner, mock_db_ops, temp_db):
    """Test monitor command error handling."""
    # Setup mock
    instance = mock_db_ops.return_value
    instance.monitor_database.side_effect = Exception("Monitoring failed")
    
    # Test non-existent database
    result = runner.invoke(cli, ['monitor', 'nonexistent.accdb'])
    assert result.exit_code == 2
    assert "Invalid value for 'DATABASE': Path 'nonexistent.accdb' does not exist" in result.output

    # Test runtime error
    result = runner.invoke(cli, ['monitor', temp_db])
    assert result.exit_code == 1
    assert 'Error: Monitoring failed' in result.output

# Command Output Formatting Tests
def test_sync_command_output_formatting(runner, mock_db_ops, temp_db, temp_excel):
    """Test sync command output formatting."""
    result = runner.invoke(cli, ['sync', temp_db, temp_excel])
    assert result.exit_code == 0
    assert 'Successfully synchronized data' in result.output

def test_validate_command_output_formatting(runner, mock_db_ops, temp_db):
    """Test validate command output formatting."""
    result = runner.invoke(cli, ['validate', temp_db])
    assert result.exit_code == 0
    assert 'Database is valid' in result.output

def test_monitor_command_output_formatting(runner, mock_db_ops, temp_db):
    """Test monitor command output formatting."""
    result = runner.invoke(cli, ['monitor', temp_db])
    assert result.exit_code == 0

# Command Integration Tests
def test_sync_command_integration(runner, mock_db_ops, temp_db, temp_excel):
    """Test sync command integration."""
    result = runner.invoke(cli, ['sync', temp_db, temp_excel])
    assert result.exit_code == 0
    instance = mock_db_ops.return_value
    instance.sync_to_excel.assert_called_once_with(temp_db, temp_excel, 1000, True)

def test_validate_command_integration(runner, mock_db_ops, temp_db):
    """Test validate command integration."""
    result = runner.invoke(cli, ['validate', temp_db])
    assert result.exit_code == 0
    instance = mock_db_ops.return_value
    instance.validate_database.assert_called_once_with(temp_db, None, None)

def test_monitor_command_integration(runner, mock_db_ops, temp_db):
    """Test monitor command integration."""
    result = runner.invoke(cli, ['monitor', temp_db])
    assert result.exit_code == 0
    instance = mock_db_ops.return_value
    instance.monitor_database.assert_called_once_with(temp_db, 60, None)

# Utility Function Tests
def test_get_destination_type():
    """Test the get_destination_type utility function."""
    assert get_destination_type('test.accdb') == 'access'
    assert get_destination_type('test.xlsx') == 'excel'
    
    with pytest.raises(click.UsageError) as excinfo:
        get_destination_type('test.txt')
    assert 'Unsupported destination type: test.txt' in str(excinfo.value)

# Cleanup Tests
def test_cleanup(temp_db, temp_excel):
    """Test cleanup of temporary files."""
    assert os.path.exists(temp_db)
    assert os.path.exists(temp_excel)
    os.remove(temp_db)
    os.remove(temp_excel)
    assert not os.path.exists(temp_db)
    assert not os.path.exists(temp_excel) 