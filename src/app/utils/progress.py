"""
Progress indicator utilities for long-running operations.
"""

import sys
import time
from threading import Thread, Event
from typing import Optional


class ProgressIndicator:
    """
    Simple text-based progress indicator for command-line interface.
    Can be used as a context manager or manually started/stopped.
    """
    
    def __init__(self, message: str = "Processing", spinner_type: str = "dots"):
        """
        Initialize progress indicator.
        
        Args:
            message: Message to display
            spinner_type: Type of spinner animation (dots, bar, clock)
        """
        self.message = message
        self.spinner_type = spinner_type
        self._stop_event = Event()
        self._thread = None
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
    
    def _get_spinner_chars(self):
        """Get spinner animation characters based on type."""
        if self.spinner_type == "dots":
            return ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        elif self.spinner_type == "bar":
            return ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█", "▇", "▆", "▅", "▄", "▃", "▁"]
        elif self.spinner_type == "clock":
            return ["🕛", "🕐", "🕑", "🕒", "🕓", "🕔", "🕕", "🕖", "🕗", "🕘", "🕙", "🕚"]
        else:
            return ["-", "\\", "|", "/"]
    
    def _spinner_task(self):
        """Background task that displays the spinner animation."""
        spinner_chars = self._get_spinner_chars()
        index = 0
        
        while not self._stop_event.is_set():
            # Print spinner with message
            sys.stdout.write(f"\r{spinner_chars[index]} {self.message}... ")
            sys.stdout.flush()
            
            # Update spinner position
            index = (index + 1) % len(spinner_chars)
            
            # Wait a short time
            time.sleep(0.1)
    
    def start(self):
        """Start the progress indicator."""
        if self._thread is None or not self._thread.is_alive():
            self._stop_event.clear()
            self._thread = Thread(target=self._spinner_task)
            self._thread.daemon = True
            self._thread.start()
    
    def stop(self, clear: bool = True):
        """
        Stop the progress indicator.
        
        Args:
            clear: Whether to clear the line after stopping
        """
        if self._thread and self._thread.is_alive():
            self._stop_event.set()
            self._thread.join()
            
            if clear:
                # Clear the line
                sys.stdout.write("\r" + " " * (len(self.message) + 15) + "\r")
            else:
                # Just move to the next line
                sys.stdout.write("\n")
            sys.stdout.flush()
    
    def update_message(self, message: str):
        """
        Update the displayed message.
        
        Args:
            message: New message to display
        """
        self.message = message


class ProgressBar:
    """
    Text-based progress bar for command-line interface.
    Shows percentage completion for operations with known total.
    """
    
    def __init__(self, total: int, message: str = "Processing", width: int = 30):
        """
        Initialize progress bar.
        
        Args:
            total: Total number of items
            message: Message to display
            width: Width of the progress bar in characters
        """
        self.total = max(1, total)  # Avoid division by zero
        self.message = message
        self.width = width
        self.current = 0
    
    def update(self, current: Optional[int] = None, increment: int = 1):
        """
        Update progress bar.
        
        Args:
            current: Current position (if None, increment by increment)
            increment: Amount to increment if current is None
        """
        if current is not None:
            self.current = current
        else:
            self.current += increment
        
        # Calculate percentage
        percentage = min(100, int((self.current / self.total) * 100))
        
        # Calculate bar filled width
        filled_width = int((self.width * self.current) // self.total)
        
        # Create the bar
        bar = "█" * filled_width + "░" * (self.width - filled_width)
        
        # Print the bar
        sys.stdout.write(f"\r{self.message}: [{bar}] {percentage}% ({self.current}/{self.total})")
        sys.stdout.flush()
    
    def finish(self):
        """Complete the progress bar and move to the next line."""
        self.update(self.total)
        sys.stdout.write("\n")
        sys.stdout.flush()


def progress_callback_factory(progress_bar: ProgressBar):
    """
    Create a callback function for progress updates.
    
    Args:
        progress_bar: ProgressBar instance to update
        
    Returns:
        Callback function that updates the progress bar
    """
    def callback(current: int, total: int):
        progress_bar.update(current)
    
    return callback 