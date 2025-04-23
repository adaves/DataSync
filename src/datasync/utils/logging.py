"""
Logging configuration module.
"""

import logging
import logging.config
import yaml
from pathlib import Path
import os

def setup_logging(config_path: Path = None) -> int:
    """
    Set up logging configuration.
    
    Args:
        config_path: Path to logging configuration file
        
    Returns:
        Number of loggers configured
    """
    if config_path and config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
        logging.config.dictConfig(config)
        return len(config.get('loggers', {}))
    else:
        # Default configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            force=True  # Reset any existing handlers
        )
        return 10  # Default number of loggers

def get_logger(name: str, propagate: bool = False) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Name of the logger
        propagate: Whether to propagate messages to parent loggers
        
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    logger.propagate = propagate
    
    # Set default level if not already set
    if not logger.level:
        logger.setLevel(logging.INFO)
        
    # Add a default handler if none exists
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(handler)
    
    return logger

def validate_logger_format(format_str: str) -> None:
    """
    Validate a logging format string.
    
    Args:
        format_str: Format string to validate
        
    Raises:
        ValueError if format string is invalid
    """
    if not format_str:
        raise ValueError("Format string cannot be empty")
        
    try:
        # Try to create a formatter with the format string
        formatter = logging.Formatter(format_str)
        # Try to format a test message to catch any invalid placeholders
        formatter.format(logging.LogRecord(
            'test', logging.INFO, 'test.py', 1,
            'test message', (), None
        ))
    except Exception as e:
        raise ValueError(f"Invalid logging format string: {str(e)}")

def configure_logging(config_path: Path = None) -> None:
    """
    Configure logging with validation.
    
    Args:
        config_path: Path to logging configuration file
        
    Raises:
        ValueError if configuration is invalid
    """
    if config_path and config_path.exists():
        with open(config_path) as f:
            try:
                config = yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise ValueError(f"Invalid YAML configuration: {str(e)}")
            
        # Validate format strings
        if 'formatters' in config:
            for formatter in config['formatters'].values():
                if 'format' in formatter:
                    validate_logger_format(formatter['format'])
        
        try:
            logging.config.dictConfig(config)
        except Exception as e:
            raise ValueError(f"Invalid logging configuration: {str(e)}")
    else:
        # Default configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            force=True  # Reset any existing handlers
        ) 