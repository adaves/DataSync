"""
Unit tests for the configuration manager.
"""

import pytest
import yaml
from pathlib import Path
from datasync.utils.config import ConfigManager
import tempfile
import os
import logging
from datasync.utils.error_handling import ConfigurationError

@pytest.fixture(autouse=True)
def clear_env_vars():
    """Clear environment variables before each test."""
    old_env = os.environ.copy()
    os.environ.clear()
    yield
    os.environ.update(old_env)

@pytest.fixture
def temp_config():
    """Create a temporary configuration file."""
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
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
        yaml.dump(config, tmp, default_flow_style=False)
        tmp_path = tmp.name
    
    yield tmp_path
    
    # Cleanup after test
    if os.path.exists(tmp_path):
        os.unlink(tmp_path)

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

def test_config_validation():
    """Test configuration validation."""
    manager = ConfigManager()
    
    # Test invalid configuration values
    with pytest.raises(ConfigurationError):
        manager.set('database.connection.timeout', -1)
    
    # Test invalid configuration structure
    with pytest.raises(ConfigurationError):
        manager.set('database.connection', 'invalid')

def test_config_updates():
    """Test configuration updates."""
    manager = ConfigManager()
    
    # Test updating existing values
    manager.set('database.connection.timeout', 60)
    assert manager.get('database.connection.timeout') == 60
    
    # Test adding new sections
    manager.set('new_section.key', 'value')
    assert manager.get('new_section.key') == 'value'

def test_environment_variables():
    """Test environment variable integration."""
    os.environ['DATASYNC_DB_DRIVER'] = 'env_driver'
    os.environ['DATASYNC_DB_TIMEOUT'] = '15'
    
    manager = ConfigManager()
    
    # Test environment variable override
    assert manager.get('database.driver') == 'env_driver'
    assert manager.get('database.connection.timeout') == 15

def test_default_values():
    """Test default value handling."""
    manager = ConfigManager()
    
    # Test default values for missing sections
    assert manager.get('non_existent_section', default={}) == {}
    assert manager.get('non_existent_key', default='default') == 'default'
    
    # Test default values for invalid types
    assert manager.get('database.connection.timeout', default='invalid') == 30

def test_error_handling():
    """Test error handling in configuration."""
    # Test invalid YAML file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
        tmp.write('invalid: yaml: content')
        tmp.flush()
        tmp_path = tmp.name
    
    try:
        with pytest.raises(yaml.YAMLError):
            ConfigManager(tmp_path)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
    
    # Test non-existent file with no defaults
    with pytest.raises(FileNotFoundError):
        ConfigManager('non_existent.yaml', use_defaults=False)
    
    # Test invalid configuration path
    with pytest.raises(ValueError):
        ConfigManager('/invalid/path/config.yaml')

def test_logging_config():
    """Test logging configuration."""
    manager = ConfigManager()
    
    # Test logging configuration
    logging_config = manager.get_logging_config()
    assert 'level' in logging_config
    assert 'file' in logging_config
    assert 'console' in logging_config
    
    # Test logging configuration updates
    manager.set('logging.level', 'DEBUG')
    assert manager.get('logging.level') == 'DEBUG'

def test_config_reload():
    """Test configuration reloading."""
    manager = ConfigManager()
    
    # Test reloading configuration
    initial_timeout = manager.get('database.connection.timeout')
    manager.set('database.connection.timeout', initial_timeout + 10)
    manager.reload()
    assert manager.get('database.connection.timeout') == initial_timeout

def test_config_validation_rules():
    """Test configuration validation rules."""
    manager = ConfigManager()
    
    # Test numeric validation
    with pytest.raises(ConfigurationError):
        manager.set('database.connection.timeout', -1)
    
    # Test string validation
    with pytest.raises(ConfigurationError):
        manager.set('database.driver', '')
    
    # Test boolean validation
    with pytest.raises(ConfigurationError):
        manager.set('sync.validation.enabled', 'invalid')

def test_config_performance():
    """Test configuration performance."""
    import time
    
    manager = ConfigManager()
    start_time = time.time()
    
    # Test multiple get operations
    for _ in range(1000):
        manager.get('database.driver')
        manager.get('database.connection.timeout')
        manager.get('sync.batch.size')
    
    end_time = time.time()
    assert (end_time - start_time) < 1.0  # Should complete in under 1 second

def test_config_integration():
    """Test configuration integration with other components."""
    manager = ConfigManager()
    
    # Test database configuration
    db_config = manager.get_database_config()
    assert 'driver' in db_config
    assert 'connection' in db_config
    
    # Test sync configuration
    sync_config = manager.get_sync_config()
    assert 'batch' in sync_config
    assert 'validation' in sync_config
    
    # Test validation configuration
    validation_config = manager.get_validation_config()
    assert 'rules' in validation_config
    
    # Test monitor configuration
    monitor_config = manager.get_monitor_config()
    assert 'interval' in monitor_config
    assert 'metrics' in monitor_config 