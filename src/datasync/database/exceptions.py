"""
Custom exceptions for database operations.
"""

class DatabaseError(Exception):
    """Base exception for database-related errors."""
    pass

class ConnectionError(DatabaseError):
    """Exception raised for database connection errors."""
    pass

class TransactionError(DatabaseError):
    """Exception raised for transaction-related errors."""
    pass

class QueryError(DatabaseError):
    """Exception raised for query execution errors."""
    pass

class ValidationError(DatabaseError):
    """Exception raised for data validation errors."""
    pass 