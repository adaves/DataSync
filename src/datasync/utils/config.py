"""
Configuration management for the DataSync application.
This module handles loading and managing settings from the YAML configuration file.
"""

import yaml
from pathlib import Path
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    """Manages application configuration settings."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to the configuration file. If None, uses default path.
        """
        self.config_path = config_path or str(Path(__file__).parent.parent.parent / "config" / "settings.yaml")
        self._config: Dict[str, Any] = {}
        self.load_config()
    
    def load_config(self) -> None:
        """Load configuration from the YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                self._config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
        except FileNotFoundError:
            logger.warning(f"Configuration file not found at {self.config_path}, using default settings")
            self._config = self._get_default_config()
        except yaml.YAMLError as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration settings."""
        return {
            'database': {
                'driver': "Microsoft Access Driver (*.mdb, *.accdb)",
                'connection': {
                    'timeout': 30,
                    'retry_attempts': 3,
                    'retry_delay': 5
                },
                'paths': {
                    'source': "data/source.accdb",
                    'destination': "data/destination.accdb",
                    'backup': "data/backups"
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
        """
        keys = key.split('.')
        current = self._config
        
        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]
        
        current[keys[-1]] = value
    
    def save(self) -> None:
        """Save the current configuration to the YAML file."""
        try:
            with open(self.config_path, 'w') as f:
                yaml.safe_dump(self._config, f, default_flow_style=False)
            logger.info(f"Configuration saved to {self.config_path}")
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            raise
    
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