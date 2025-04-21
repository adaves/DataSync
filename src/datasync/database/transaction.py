"""
Transaction management for database operations.
This module provides a TransactionManager class to handle transaction state and operations.
"""

from typing import Optional, Callable
import logging
from contextlib import contextmanager

class TransactionManager:
    """Manages database transactions and their state."""
    
    def __init__(self, connection, logger: Optional[logging.Logger] = None):
        """
        Initialize the transaction manager.
        
        Args:
            connection: Database connection object
            logger: Optional logger instance
        """
        self.connection = connection
        self.logger = logger or logging.getLogger(__name__)
        self._transaction_count = 0
        self._in_transaction = False
        
    @property
    def in_transaction(self) -> bool:
        """Check if currently in a transaction."""
        return self._in_transaction
    
    @property
    def transaction_count(self) -> int:
        """Get the current transaction nesting level."""
        return self._transaction_count
    
    def begin(self) -> None:
        """Begin a new transaction."""
        if not self._in_transaction:
            self.connection.autocommit = False
            self._in_transaction = True
            self.logger.debug("Transaction started")
        self._transaction_count += 1
        
    def commit(self) -> None:
        """Commit the current transaction."""
        if self._transaction_count > 0:
            self._transaction_count -= 1
            if self._transaction_count == 0:
                self.connection.commit()
                self.connection.autocommit = True
                self._in_transaction = False
                self.logger.debug("Transaction committed")
                
    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self._in_transaction:
            self.connection.rollback()
            self.connection.autocommit = True
            self._in_transaction = False
            self._transaction_count = 0
            self.logger.debug("Transaction rolled back")
            
    @contextmanager
    def transaction(self):
        """
        Context manager for transaction handling.
        
        Usage:
            with transaction_manager.transaction():
                # Perform database operations
                pass
        """
        self.begin()
        try:
            yield
            self.commit()
        except Exception:
            self.rollback()
            raise 