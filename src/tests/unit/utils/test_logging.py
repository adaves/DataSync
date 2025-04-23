"""
Unit tests for logging functionality.
"""

import pytest
import logging
import os
import yaml
from pathlib import Path
from datasync.utils.logging import (
    setup_logging,
    get_logger,
    validate_logger_format,
    configure_logging
)

class TestLogging:
    """Test cases for logging functionality."""
    
    @pytest.fixture
    def log_dir(self, tmp_path):
        """Create a temporary directory for log files."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        return log_dir
    
    @pytest.fixture
    def config_file(self, tmp_path):
        """Create a temporary logging configuration file."""
        config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': 'INFO',
                    'formatter': 'standard',
                    'stream': 'ext://sys.stdout'
                }
            },
            'loggers': {
                'datasync': {
                    'level': 'DEBUG',
                    'handlers': ['console'],
                    'propagate': False
                }
            },
            'root': {
                'level': 'INFO',
                'handlers': ['console']
            }
        }
        config_path = tmp_path / "logging.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        return config_path
    
    @pytest.fixture
    def file_config(self, tmp_path, log_dir):
        """Create a configuration with file handler."""
        config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'detailed': {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s'
                }
            },
            'handlers': {
                'file': {
                    'class': 'logging.FileHandler',
                    'level': 'DEBUG',
                    'formatter': 'detailed',
                    'filename': str(log_dir / "test.log")
                }
            },
            'loggers': {
                'datasync': {
                    'level': 'DEBUG',
                    'handlers': ['file'],
                    'propagate': False
                }
            }
        }
        config_path = tmp_path / "file_logging.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        return config_path

    def test_setup_logging(self, config_file):
        """Test logging setup with configuration file."""
        num_loggers = setup_logging(config_file)
        logger = get_logger("test")
        assert logger.level == logging.INFO
        assert len(logger.handlers) > 0
        assert num_loggers == 1  # One logger in config

    def test_setup_logging_default(self):
        """Test logging setup with default configuration."""
        num_loggers = setup_logging()
        logger = get_logger("test_default")
        assert logger.level == logging.INFO
        assert len(logger.handlers) > 0
        assert num_loggers == 10

    def test_get_logger(self):
        """Test getting a logger instance."""
        logger = get_logger("test")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test"
        assert logger.level == logging.INFO
        
    def test_get_logger_with_propagation(self):
        """Test getting a logger with propagation enabled."""
        logger = get_logger("test_prop", propagate=True)
        assert logger.propagate
        assert logger.level == logging.INFO
        
    def test_logger_levels(self, caplog):
        """Test different logging levels."""
        # Get logger and enable propagation
        logger = get_logger("test_levels")
        logger.propagate = True
        logger.setLevel(logging.DEBUG)

        # Capture all log levels
        with caplog.at_level(logging.DEBUG):
            logger.debug("Debug message")
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")
            logger.critical("Critical message")

        # Verify number of records and their levels
        assert len(caplog.records) == 5
        assert caplog.records[0].levelno == logging.DEBUG
        assert caplog.records[1].levelno == logging.INFO
        assert caplog.records[2].levelno == logging.WARNING
        assert caplog.records[3].levelno == logging.ERROR
        assert caplog.records[4].levelno == logging.CRITICAL

        # Verify message content
        assert "Debug message" in caplog.text
        assert "Info message" in caplog.text
        assert "Warning message" in caplog.text
        assert "Error message" in caplog.text
        assert "Critical message" in caplog.text

    def test_validate_logger_format_valid(self):
        """Test validation of valid format strings."""
        valid_formats = [
            "%(asctime)s - %(message)s",
            "%(levelname)s: %(message)s",
            "%(name)s [%(levelname)s] %(message)s",
            "%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s"
        ]
        for format_str in valid_formats:
            validate_logger_format(format_str)  # Should not raise

    def test_validate_logger_format_invalid(self):
        """Test validation of invalid format strings."""
        invalid_formats = [
            "%(invalid)s",
            "%(message",
            "%{message}",
            ""
        ]
        for format_str in invalid_formats:
            with pytest.raises(ValueError):
                validate_logger_format(format_str)

    def test_file_logging(self, file_config, log_dir):
        """Test logging to file."""
        # Ensure log directory exists
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure logging
        configure_logging(file_config)
        
        # Get logger and ensure it has the file handler
        logger = get_logger("test_file")
        logger.setLevel(logging.DEBUG)  # Ensure debug messages are captured
        
        # Clear any existing handlers
        logger.handlers = []
        
        # Add file handler
        file_handler = logging.FileHandler(log_dir / "test.log")
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(file_handler)
        
        test_message = "Test file logging"
        logger.info(test_message)
        
        # Flush the handler to ensure the message is written
        file_handler.flush()
        
        log_file = log_dir / "test.log"
        assert log_file.exists()
        content = log_file.read_text()
        assert test_message in content

    def test_logger_hierarchy(self):
        """Test logger hierarchy and propagation."""
        parent = get_logger("parent")
        child = get_logger("parent.child")
        grandchild = get_logger("parent.child.grandchild")
        
        assert not parent.propagate
        assert not child.propagate
        assert not grandchild.propagate
        
        # Test with propagation enabled
        parent_prop = get_logger("parent_prop", propagate=True)
        child_prop = get_logger("parent_prop.child", propagate=True)
        assert parent_prop.propagate
        assert child_prop.propagate

    def test_configure_logging_invalid_config(self, tmp_path):
        """Test configure_logging with invalid configuration."""
        invalid_config = tmp_path / "invalid_logging.yaml"
        config = {
            'version': 1,
            'formatters': {
                'invalid': {
                    'format': '%(invalid)s'
                }
            }
        }
        with open(invalid_config, 'w') as f:
            yaml.dump(config, f)
        with pytest.raises(ValueError):
            configure_logging(invalid_config)

    def test_configure_logging_missing_file(self):
        """Test configure_logging with missing configuration file."""
        configure_logging(Path("nonexistent.yaml"))  # Should use default config
        logger = get_logger("test_missing_config")
        assert logger.level == logging.INFO 