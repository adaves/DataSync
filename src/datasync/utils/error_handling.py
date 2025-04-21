"""
Error handling framework for the DataSync application.
This module provides base error classes and utilities for consistent error handling.
"""

from typing import Optional, Dict, Any, Type, Callable
import logging
from pathlib import Path
import functools
import traceback
from datetime import datetime

class DataSyncError(Exception):
    """Base exception class for all DataSync errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize the error with a message and optional details.
        
        Args:
            message: Human-readable error message
            details: Optional dictionary of additional error details
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.timestamp = datetime.now()
        self.traceback = traceback.format_exc()
        self.logger = logging.getLogger(__name__)

class DatabaseError(DataSyncError):
    """Base exception for database-related errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None, 
                 query: Optional[str] = None, params: Optional[Dict[str, Any]] = None):
        """
        Initialize database error with query information.
        
        Args:
            message: Error message
            details: Additional error details
            query: SQL query that caused the error
            params: Query parameters
        """
        super().__init__(message, details)
        self.query = query
        self.params = params

class ConnectionError(DatabaseError):
    """Database connection-related errors."""
    pass

class TransactionError(DatabaseError):
    """Transaction-related errors."""
    pass

class QueryError(DatabaseError):
    """Query execution errors."""
    pass

class ValidationError(DataSyncError):
    """Base exception for validation-related errors."""
    
    def __init__(self, message: str, field: Optional[str] = None, 
                 value: Optional[Any] = None, details: Optional[Dict[str, Any]] = None):
        """
        Initialize validation error.
        
        Args:
            message: Error message
            field: Name of the field that failed validation
            value: Invalid value
            details: Additional error details
        """
        super().__init__(message, details)
        self.field = field
        self.value = value

class ConfigurationError(DataSyncError):
    """Base exception for configuration-related errors."""
    pass

class FileError(DataSyncError):
    """Base exception for file-related errors."""
    
    def __init__(self, message: str, file_path: Optional[Path] = None, 
                 details: Optional[Dict[str, Any]] = None):
        """
        Initialize file error.
        
        Args:
            message: Error message
            file_path: Path to the file that caused the error
            details: Additional error details
        """
        super().__init__(message, details)
        self.file_path = file_path

class ErrorHandler:
    """Utility class for handling and logging errors."""
    
    def __init__(self, log_file: Optional[Path] = None, 
                 error_map: Optional[Dict[Type[Exception], Type[DataSyncError]]] = None):
        """
        Initialize the error handler.
        
        Args:
            log_file: Optional path to the log file
            error_map: Optional mapping of external exceptions to DataSync exceptions
        """
        self.logger = logging.getLogger(__name__)
        self.error_map = error_map or {}
        if log_file:
            self._setup_file_logging(log_file)
    
    def _setup_file_logging(self, log_file: Path) -> None:
        """Setup file logging for errors."""
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(file_handler)
    
    def handle_error(self, error: Exception, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Handle and log an error with context.
        
        Args:
            error: The exception to handle
            context: Optional dictionary of context information
        """
        context = context or {}
        
        if isinstance(error, DataSyncError):
            # Log DataSync errors with their details
            self.logger.error(
                f"{error.__class__.__name__}: {error.message}",
                extra={
                    'details': error.details,
                    'context': context,
                    'timestamp': error.timestamp,
                    'traceback': error.traceback
                }
            )
        else:
            # Map external exceptions to DataSync exceptions if possible
            mapped_error = self._map_error(error)
            if mapped_error:
                self.handle_error(mapped_error, context)
            else:
                # Log other exceptions
                self.logger.error(
                    f"Unexpected error: {str(error)}",
                    extra={'context': context},
                    exc_info=True
                )
    
    def _map_error(self, error: Exception) -> Optional[DataSyncError]:
        """Map external exceptions to DataSync exceptions."""
        for error_type, datasync_error in self.error_map.items():
            if isinstance(error, error_type):
                return datasync_error(str(error))
        return None
    
    def log_warning(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Log a warning message with context.
        
        Args:
            message: Warning message
            context: Optional dictionary of context information
        """
        self.logger.warning(message, extra={'context': context or {}})
    
    def log_info(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Log an info message with context.
        
        Args:
            message: Info message
            context: Optional dictionary of context information
        """
        self.logger.info(message, extra={'context': context or {}})

def handle_database_error(func: Callable) -> Callable:
    """
    Decorator for handling database operation errors.
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function with error handling
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Get context information
            context = {
                'function': func.__name__,
                'args': args,
                'kwargs': kwargs
            }
            
            # Create appropriate error type
            if 'connection' in str(e).lower():
                error = ConnectionError(str(e), context)
            elif 'transaction' in str(e).lower():
                error = TransactionError(str(e), context)
            elif 'query' in str(e).lower() or 'sql' in str(e).lower():
                error = QueryError(str(e), context)
            else:
                error = DatabaseError(str(e), context)
            
            # Log the error
            error.logger.error(
                f"Database operation failed: {error.message}",
                extra={
                    'details': error.details,
                    'timestamp': error.timestamp,
                    'traceback': error.traceback
                }
            )
            
            raise error
    
    return wrapper

def handle_validation_error(func: Callable) -> Callable:
    """
    Decorator for handling validation errors.
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function with error handling
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            context = {
                'function': func.__name__,
                'args': args,
                'kwargs': kwargs
            }
            error = ValidationError(str(e), details=context)
            error.logger.error(
                f"Validation failed: {error.message}",
                extra={
                    'details': error.details,
                    'timestamp': error.timestamp,
                    'traceback': error.traceback
                }
            )
            raise error
    
    return wrapper

def handle_file_error(func: Callable) -> Callable:
    """
    Decorator for handling file operation errors.
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function with error handling
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            context = {
                'function': func.__name__,
                'args': args,
                'kwargs': kwargs
            }
            error = FileError(str(e), details=context)
            error.logger.error(
                f"File operation failed: {error.message}",
                extra={
                    'details': error.details,
                    'timestamp': error.timestamp,
                    'traceback': error.traceback
                }
            )
            raise error
    
    return wrapper 