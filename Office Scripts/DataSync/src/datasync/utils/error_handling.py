"""
Error handling utilities for the DataSync package.
"""

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple, Type, Union
from functools import wraps

logger = logging.getLogger(__name__)

class DataSyncError(Exception):
    """Base exception class for DataSync application errors."""
    pass

class ConfigurationError(DataSyncError):
    """Exception raised for configuration-related errors."""
    pass

class DatabaseError(DataSyncError):
    """Exception raised for database-related errors."""
    pass

class ValidationError(DataSyncError):
    """Exception raised for validation-related errors."""
    pass

class FileError(DataSyncError):
    """Exception raised for file-related errors."""
    pass

class ProcessingError(DataSyncError):
    """Exception raised for data processing errors."""
    pass

class SyncError(DataSyncError):
    """Exception raised for synchronization errors."""
    pass

class MonitorError(DataSyncError):
    """Exception raised for monitoring errors."""
    pass

@dataclass
class ErrorContext:
    """Context information for error handling."""
    
    operation: str
    details: Dict[str, Any] = None
    timestamp: str = None
    
    def __post_init__(self):
        """Initialize default values."""
        if self.details is None:
            self.details = {}
        if self.timestamp is None:
            self.timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary."""
        return {
            "operation": self.operation,
            "details": self.details,
            "timestamp": self.timestamp
        }

@dataclass
class RetryConfig:
    """Configuration for retry operations."""
    
    max_attempts: int = 3
    delay: float = 1.0
    backoff_factor: float = 2.0
    exceptions: Tuple[Type[Exception], ...] = (Exception,)

class ErrorHandler:
    """Handler for managing and processing errors."""
    
    def __init__(self):
        """Initialize error handler."""
        self._handlers: Dict[Type[Exception], Callable] = {}
    
    def register_handler(self, exception_type: Type[Exception], handler: Callable):
        """Register a custom handler for an exception type."""
        self._handlers[exception_type] = handler
    
    def handle_error(self, error: Exception, context: ErrorContext) -> Dict[str, Any]:
        """Handle an error with the given context."""
        # Try to find a specific handler for the error type
        for exception_type, handler in self._handlers.items():
            if isinstance(error, exception_type):
                return handler(error, context)
        
        # Default handling
        return {
            "error": str(error),
            "context": context.to_dict(),
            "type": error.__class__.__name__
        }

def retry_on_error(
    func: Optional[Callable] = None,
    *,
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Callable:
    """
    Decorator for retrying operations on error.
    
    Args:
        func: The function to decorate
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff_factor: Factor to multiply delay by after each retry
        exceptions: Tuple of exception types to catch and retry
    
    Returns:
        Decorated function
    """
    if func is None:
        return lambda f: retry_on_error(
            f,
            max_attempts=max_attempts,
            delay=delay,
            backoff_factor=backoff_factor,
            exceptions=exceptions
        )
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        last_exception = None
        current_delay = delay
        
        for attempt in range(max_attempts):
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                last_exception = e
                if attempt < max_attempts - 1:
                    time.sleep(current_delay)
                    current_delay *= backoff_factor
        
        raise last_exception
    
    return wrapper

def _safe_rollback(obj):
    """
    Safely roll back a transaction without triggering recursion.
    
    Args:
        obj: Object that has rollback methods
    """
    # Attribute priority order for safety
    if hasattr(obj, '_in_rollback') and obj._in_rollback:
        # Already in rollback state, skip to prevent recursion
        logger.debug("Skipping rollback as already in progress")
        return
        
    # Try the class's preferred rollback method first
    if hasattr(obj, 'rollback_transaction'):
        try:
            obj.rollback_transaction()
        except Exception as e:
            logger.error(f"Error in rollback_transaction during error handling: {str(e)}")
    # Fallback to standard rollback method if needed
    elif hasattr(obj, 'rollback'):
        try:
            obj.rollback()
        except Exception as e:
            logger.error(f"Error in rollback during error handling: {str(e)}")

def handle_error(func: Optional[Callable] = None, *, operation: Optional[str] = None) -> Union[Dict[str, Any], Callable]:
    """
    Handle an error with context. Can be used as a function or decorator.
    
    When used as a function:
        result = handle_error(error, context)
    
    When used as a decorator:
        @handle_error(operation="operation_name")
        def some_function():
            ...
    
    Args:
        func: The function to decorate or the error to handle
        operation: Optional operation name for the decorator
    
    Returns:
        When used as a function: Dictionary with error information
        When used as a decorator: Decorated function
    """
    if func is None:
        # Being used as a decorator with parameters
        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    # Try to safely handle any ongoing database transactions
                    if args and len(args) > 0:
                        _safe_rollback(args[0])
                    
                    context = ErrorContext(
                        operation=operation or f.__name__,
                        details={
                            'args': args,
                            'kwargs': kwargs,
                            'function': f.__name__
                        }
                    )
                    handler = ErrorHandler()
                    result = handler.handle_error(e, context)
                    log_error(e, context)
                    raise type(e)(str(e)).with_traceback(e.__traceback__)
            return wrapper
        return decorator
    
    if isinstance(func, Exception):
        # Being used as a function
        context = ErrorContext(operation=operation or "unknown_operation")
        handler = ErrorHandler()
        return handler.handle_error(func, context)
    
    # Being used as a decorator without parameters
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Try to safely handle any ongoing database transactions
            if args and len(args) > 0:
                _safe_rollback(args[0])
            
            context = ErrorContext(
                operation=func.__name__,
                details={
                    'args': args,
                    'kwargs': kwargs,
                    'function': func.__name__
                }
            )
            handler = ErrorHandler()
            result = handler.handle_error(e, context)
            log_error(e, context)
            raise type(e)(str(e)).with_traceback(e.__traceback__)
    return wrapper

def format_error_message(error: Exception, context: ErrorContext) -> str:
    """
    Format an error message with context.
    
    Args:
        error: The exception
        context: Error context information
    
    Returns:
        Formatted error message
    """
    return (
        f"Error in {context.operation}: {str(error)}\n"
        f"Context: {context.to_dict()}"
    )

def log_error(error: Exception, context: ErrorContext):
    """
    Log an error with context.
    
    Args:
        error: The exception to log
        context: Error context information
    """
    message = format_error_message(error, context)
    logger.error(message) 