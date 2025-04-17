import pytest
import logging
import os
from pathlib import Path
from src.datasync.utils.logging import setup_logging, get_logger

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
        config = tmp_path / "logging.yaml"
        config.write_text("""
version: 1
disable_existing_loggers: false
formatters:
  standard:
    format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
handlers:
  console:
    class: logging.StreamHandler
    level: INFO
    formatter: standard
    stream: ext://sys.stdout
loggers:
  datasync:
    level: DEBUG
    handlers: [console]
    propagate: false
root:
  level: INFO
  handlers: [console]
""")
        return config
    
    def test_setup_logging(self, config_file):
        """Test logging setup with configuration file."""
        setup_logging(config_file)
        logger = get_logger("test")
        assert logger.level == logging.DEBUG
        assert len(logger.handlers) > 0
    
    def test_get_logger(self):
        """Test getting a logger instance."""
        logger = get_logger("test")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "datasync.test"
    
    def test_logger_levels(self):
        """Test different logging levels."""
        logger = get_logger("test_levels")
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        logger.critical("Critical message")
    
    def test_logger_format(self):
        """Test log message formatting."""
        logger = get_logger("test_format")
        with pytest.raises(ValueError):
            logger.error("Test error: %s", "error message", extra={"invalid": "format"})
    
    def test_logger_propagation(self):
        """Test logger propagation settings."""
        logger = get_logger("test_propagation")
        assert not logger.propagate 