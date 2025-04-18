"""
Logging configuration module.
"""

import logging
import logging.config
import yaml
from pathlib import Path

def setup_logging(config_path: Path = None) -> None:
    """
    Set up logging configuration.
    
    Args:
        config_path: Path to logging configuration file
    """
    if config_path and config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    else:
        # Default configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Name of the logger
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name) 