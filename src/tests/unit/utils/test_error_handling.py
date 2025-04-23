"""
Unit tests for error handling utilities.
"""

import pytest
from datasync.utils.error_handling import (
    ErrorContext,
    ErrorHandler,
    RetryConfig,
    retry_on_error,
    handle_error,
    log_error,
    format_error_message
)

class TestErrorContext:
    """Test cases for ErrorContext class."""
    
    def test_error_context_creation(self):
        """Test creating an error context with basic information."""
        context = ErrorContext(
            operation="test_operation",
            details={"key": "value"},
            timestamp="2024-01-01T00:00:00"
        )
        assert context.operation == "test_operation"
        assert context.details == {"key": "value"}
        assert context.timestamp == "2024-01-01T00:00:00"
    
    def test_error_context_to_dict(self):
        """Test converting error context to dictionary."""
        context = ErrorContext(
            operation="test_operation",
            details={"key": "value"},
            timestamp="2024-01-01T00:00:00"
        )
        context_dict = context.to_dict()
        assert context_dict["operation"] == "test_operation"
        assert context_dict["details"] == {"key": "value"}
        assert context_dict["timestamp"] == "2024-01-01T00:00:00"

class TestErrorHandler:
    """Test cases for ErrorHandler class."""
    
    @pytest.fixture
    def error_handler(self):
        """Create a test error handler."""
        return ErrorHandler()
    
    def test_handle_error_basic(self, error_handler):
        """Test basic error handling."""
        error = ValueError("Test error")
        context = ErrorContext(operation="test")
        result = error_handler.handle_error(error, context)
        assert isinstance(result, dict)
        assert "error" in result
        assert "context" in result
    
    def test_handle_error_with_custom_handler(self, error_handler):
        """Test error handling with custom handler."""
        def custom_handler(error, context):
            return {"custom": True, "error": str(error)}
        
        error_handler.register_handler(ValueError, custom_handler)
        error = ValueError("Test error")
        context = ErrorContext(operation="test")
        result = error_handler.handle_error(error, context)
        assert result["custom"] is True
        assert result["error"] == "Test error"

class TestRetryConfig:
    """Test cases for RetryConfig class."""
    
    def test_retry_config_creation(self):
        """Test creating a retry configuration."""
        config = RetryConfig(
            max_attempts=3,
            delay=1.0,
            backoff_factor=2.0,
            exceptions=(ValueError, TypeError)
        )
        assert config.max_attempts == 3
        assert config.delay == 1.0
        assert config.backoff_factor == 2.0
        assert ValueError in config.exceptions
        assert TypeError in config.exceptions
    
    def test_retry_config_defaults(self):
        """Test retry configuration defaults."""
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.delay == 1.0
        assert config.backoff_factor == 2.0
        assert Exception in config.exceptions

class TestRetryDecorator:
    """Test cases for retry_on_error decorator."""
    
    def test_retry_success(self):
        """Test retry decorator with successful operation."""
        @retry_on_error()
        def successful_operation():
            return "success"
        
        result = successful_operation()
        assert result == "success"
    
    def test_retry_failure(self):
        """Test retry decorator with failing operation."""
        attempts = 0
        
        @retry_on_error(max_attempts=2)
        def failing_operation():
            nonlocal attempts
            attempts += 1
            raise ValueError("Test error")
        
        with pytest.raises(ValueError):
            failing_operation()
        assert attempts == 2

class TestErrorFormatting:
    """Test cases for error formatting utilities."""
    
    def test_format_error_message(self):
        """Test formatting error messages."""
        error = ValueError("Test error")
        context = ErrorContext(
            operation="test_operation",
            details={"key": "value"}
        )
        message = format_error_message(error, context)
        assert "Test error" in message
        assert "test_operation" in message
        assert "key" in message
        assert "value" in message
    
    def test_log_error(self, caplog):
        """Test error logging."""
        error = ValueError("Test error")
        context = ErrorContext(operation="test")
        log_error(error, context)
        assert "Test error" in caplog.text
        assert "test" in caplog.text

class TestErrorHandlingIntegration:
    """Integration tests for error handling utilities."""
    
    def test_error_handling_flow(self, caplog):
        """Test complete error handling flow."""
        error_handler = ErrorHandler()
        
        @retry_on_error(max_attempts=2)
        def test_operation():
            raise ValueError("Test error")
        
        try:
            test_operation()
        except ValueError as error:
            context = ErrorContext(operation="test_flow")
            result = error_handler.handle_error(error, context)
            log_error(error, context)
        
        assert "Test error" in caplog.text
        assert "test_flow" in caplog.text
        assert isinstance(result, dict)
        assert "error" in result
        assert "context" in result 