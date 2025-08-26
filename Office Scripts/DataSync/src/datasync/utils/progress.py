"""
Progress tracking module for logging and displaying operation progress.
"""

import sys
from typing import Optional, Callable, Any
import click

class ProgressTracker:
    """Simple progress tracker for database operations."""
    
    def __init__(self, total: int, operation: str, use_click: bool = True):
        """
        Initialize the progress tracker.
        
        Args:
            total: Total number of operations to track
            operation: Name of the operation being performed
            use_click: Whether to use click.echo for output (True) or print (False)
        """
        self.total = total
        self.current = 0
        self.operation = operation
        self.use_click = use_click
        
    def update(self, increment: int = 1, message: Optional[str] = None) -> None:
        """
        Update the progress tracker.
        
        Args:
            increment: How much to increment the counter by
            message: Optional custom message to display
        """
        self.current += increment
        if self.current > self.total:
            self.current = self.total
            
        percent = (self.current / self.total) * 100 if self.total > 0 else 0
        
        if message:
            status = f"{message} ({self.current} of {self.total}, {percent:.1f}%)"
        else:
            status = f"{self.operation}: {self.current} of {self.total} ({percent:.1f}%)"
            
        if self.use_click:
            click.echo(status)
        else:
            print(status, flush=True)
            
    def complete(self, message: Optional[str] = None) -> None:
        """
        Mark the progress as complete.
        
        Args:
            message: Optional completion message
        """
        self.current = self.total
        complete_msg = message or f"{self.operation} completed ({self.total} items processed)"
        
        if self.use_click:
            click.echo(complete_msg)
        else:
            print(complete_msg, flush=True)


def track_operation(iterable: Any, operation: str, total: Optional[int] = None, 
                   step: int = 1, use_click: bool = True) -> Any:
    """
    Generator to track progress through an iterable.
    
    Args:
        iterable: Iterable to track progress through
        operation: Name of the operation being performed
        total: Total number of items (calculated from iterable if not provided)
        step: How often to report progress (every N items)
        use_click: Whether to use click for output
        
    Yields:
        Items from the original iterable
    """
    # Get total if not provided
    if total is None:
        try:
            total = len(iterable)
        except (TypeError, AttributeError):
            # If we can't determine the length, don't show percentage
            total = 0
    
    tracker = ProgressTracker(total, operation, use_click)
    count = 0
    
    for item in iterable:
        yield item
        count += 1
        
        if count % step == 0 or count == total:
            tracker.update(step)
    
    if count != total:
        tracker.complete() 