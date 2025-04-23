"""
Configuration management for the DataSync application.
This module handles loading and managing settings from the YAML configuration file.
"""

import yaml
from pathlib import Path
from typing import Any, Dict, Optional
import logging
import os
from datasync.utils.error_handling import ConfigurationError

logger = logging.getLogger(__name__)

class ConfigManager:
    """Manages application configuration settings."""
    
    def __init__(self, config_path: Optional[str] = None, use_defaults: bool = True):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to the configuration file. If None, uses default path.
            use_defaults: Whether to use default values if config file not found
            
        Raises:
            ValueError: If the configuration path is invalid
        """
        if config_path and not os.path.isabs(config_path):
            config_path = os.path.abspath(config_path)
        
        if config_path and os.path.dirname(config_path) and not os.path.exists(os.path.dirname(config_path)):
            raise ValueError(f"Invalid configuration path: {config_path}")
            
        self.config_path = config_path or str(Path(__file__).parent.parent.parent / "config" / "settings.yaml")
        self._config: Dict[str, Any] = {}
        self.use_defaults = use_defaults
        self.load_config()
    
    def load_config(self) -> None:
        """Load configuration from the YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                self._config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
        except FileNotFoundError:
            if not self.use_defaults:
                raise
            logger.warning(f"Configuration file not found at {self.config_path}, using default settings")
            self._config = self._get_default_config()
        except yaml.YAMLError as e:
            logger.error(f"Error loading configuration: {e}")
            raise
        except PermissionError as e:
            logger.error(f"Permission denied when accessing {self.config_path}: {e}")
            raise
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration settings."""
        return {
            'database': {
                'driver': os.environ.get('DATASYNC_DB_DRIVER', "Microsoft Access Driver (*.mdb, *.accdb)"),
                'connection': {
                    'timeout': int(os.environ.get('DATASYNC_DB_TIMEOUT', 30)),
                    'retry_attempts': 3,
                    'retry_delay': 5
                },
                'paths': {
                    'source': "data/source.accdb",
                    'destination': "data/destination.accdb",
                    'backup': "data/backups"
                }
            },
            'sync': {
                'batch': {
                    'size': 1000,
                    'timeout': 300
                },
                'validation': {
                    'enabled': True,
                    'check_null_values': True,
                    'check_data_types': True,
                    'check_required_fields': True
                }
            },
            'validation': {
                'rules': {
                    'fields': {
                        'required': True,
                        'max_length': 255,
                        'min_length': 1
                    },
                    'types': {
                        'integer': {
                            'min': -2147483648,
                            'max': 2147483647
                        },
                        'decimal': {
                            'precision': 18,
                            'scale': 2
                        },
                        'date': {
                            'format': "%Y-%m-%d"
                        },
                        'datetime': {
                            'format': "%Y-%m-%d %H:%M:%S"
                        }
                    },
                    'values': {
                        'allow_null': False,
                        'allow_empty': False,
                        'allow_whitespace': False
                    }
                }
            },
            'monitor': {
                'interval': 60,
                'duration': 3600,
                'metrics': [
                    'record_count',
                    'table_size',
                    'last_updated',
                    'error_count'
                ],
                'alerts': {
                    'error_threshold': 10,
                    'size_threshold': 1073741824,
                    'update_threshold': 86400
                }
            },
            'logging': {
                'level': 'INFO',
                'file': {
                    'enabled': True,
                    'path': 'logs/datasync.log',
                    'max_size': 10485760,
                    'backup_count': 5
                },
                'console': {
                    'enabled': True,
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                }
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            key: Dot-notation path to the configuration value (e.g., 'database.connection.timeout')
            default: Default value to return if the key is not found
            
        Returns:
            The configuration value or default if not found
        """
        try:
            value = self._config
            for k in key.split('.'):
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value.
        
        Args:
            key: Dot-notation path to the configuration value
            value: Value to set
            
        Raises:
            ConfigurationError if the value is invalid
        """
        self._validate_value(key, value)
        
        keys = key.split('.')
        current = self._config
        
        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            elif not isinstance(current[k], dict):
                raise ConfigurationError(f"Cannot set value at {key}: parent node is not a dictionary")
            current = current[k]
        
        # Validate that we're not replacing a dictionary with a non-dictionary
        if keys[-1] in current and isinstance(current[keys[-1]], dict) and not isinstance(value, dict):
            raise ConfigurationError(f"Cannot replace dictionary at {key} with non-dictionary value")
        
        current[keys[-1]] = value
    
    def _validate_value(self, key: str, value: Any) -> None:
        """
        Validate a configuration value.
        
        Args:
            key: Dot-notation path to the configuration value
            value: Value to validate
            
        Raises:
            ConfigurationError if the value is invalid
        """
        if key.endswith('.timeout') and isinstance(value, (int, float)) and value < 0:
            raise ConfigurationError(f"Invalid timeout value: {value}")
        
        if key.endswith('.driver') and not value:
            raise ConfigurationError("Driver cannot be empty")
        
        if key.endswith('.enabled') and not isinstance(value, bool):
            raise ConfigurationError(f"Invalid boolean value: {value}")
        
        # Check if we're trying to set a non-dictionary value where a dictionary is expected
        current = self._config
        for k in key.split('.')[:-1]:
            if k in current and isinstance(current[k], dict):
                # Get the current value at this level
                current_value = current[k].get(key.split('.')[-1])
                # Only raise error if we're trying to replace a dictionary with a non-dictionary
                if isinstance(current_value, dict) and not isinstance(value, dict):
                    raise ConfigurationError(f"Cannot set non-dictionary value at {key}")
            current = current.get(k, {})
    
    def save(self) -> None:
        """Save the current configuration to the YAML file."""
        try:
            with open(self.config_path, 'w') as f:
                yaml.safe_dump(self._config, f, default_flow_style=False)
            logger.info(f"Configuration saved to {self.config_path}")
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            raise
    
    def reload(self) -> None:
        """Reload the configuration from the file."""
        self.load_config()
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration settings."""
        return self.get('database', {})
    
    def get_sync_config(self) -> Dict[str, Any]:
        """Get synchronization configuration settings."""
        return self.get('sync', {})
    
    def get_validation_config(self) -> Dict[str, Any]:
        """Get validation configuration settings."""
        return self.get('validation', {})
    
    def get_monitor_config(self) -> Dict[str, Any]:
        """Get monitoring configuration settings."""
        return self.get('monitor', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration settings."""
        return self.get('logging', {})
    
    def get_excel_config(self) -> Dict[str, Any]:
        """Get Excel export configuration settings."""
        return self.get('excel', {})

# Create a global configuration instance
config = ConfigManager() 