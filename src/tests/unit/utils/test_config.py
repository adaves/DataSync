"""
Unit tests for the configuration manager.
"""

import pytest
import yaml
from pathlib import Path
from datasync.utils.config import ConfigManager
import tempfile
import os

@pytest.fixture
def temp_config():
    """Create a temporary configuration file."""
    with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False) as tmp:
        config = {
            'database': {
                'driver': 'test_driver',
                'connection': {
                    'timeout': 10
                }
            },
            'sync': {
                'batch': {
                    'size': 100
                }
            }
        }
        yaml.dump(config, tmp)
        return tmp.name

@pytest.fixture
def config_manager(temp_config):
    """Create a ConfigManager instance with the temporary config file."""
    return ConfigManager(temp_config)

def test_load_config(config_manager, temp_config):
    """Test loading configuration from file."""
    assert config_manager.get('database.driver') == 'test_driver'
    assert config_manager.get('database.connection.timeout') == 10
    assert config_manager.get('sync.batch.size') == 100

def test_get_default_value(config_manager):
    """Test getting default values for non-existent keys."""
    assert config_manager.get('non.existent.key', 'default') == 'default'
    assert config_manager.get('database.non.existent', None) is None

def test_set_value(config_manager):
    """Test setting configuration values."""
    config_manager.set('test.key', 'value')
    assert config_manager.get('test.key') == 'value'
    
    config_manager.set('database.connection.timeout', 20)
    assert config_manager.get('database.connection.timeout') == 20

def test_save_config(config_manager, temp_config):
    """Test saving configuration to file."""
    config_manager.set('new.key', 'new_value')
    config_manager.save()
    
    # Reload the config file
    with open(temp_config, 'r') as f:
        saved_config = yaml.safe_load(f)
    
    assert saved_config['new']['key'] == 'new_value'

def test_get_section_configs(config_manager):
    """Test getting section-specific configurations."""
    db_config = config_manager.get_database_config()
    assert db_config['driver'] == 'test_driver'
    
    sync_config = config_manager.get_sync_config()
    assert sync_config['batch']['size'] == 100
    
    # Test sections that don't exist in the test config
    assert config_manager.get_validation_config() == {}
    assert config_manager.get_monitor_config() == {}
    assert config_manager.get_logging_config() == {}
    assert config_manager.get_excel_config() == {}

def test_default_config():
    """Test using default configuration when no file exists."""
    manager = ConfigManager('non_existent_file.yaml')
    assert manager.get('database.driver') == "Microsoft Access Driver (*.mdb, *.accdb)"
    assert manager.get('database.connection.timeout') == 30

def test_cleanup(temp_config):
    """Clean up temporary files after tests."""
    if os.path.exists(temp_config):
        os.unlink(temp_config) 