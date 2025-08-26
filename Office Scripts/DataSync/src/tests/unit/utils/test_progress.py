"""
Unit tests for the progress tracking module.
This module contains tests for the ProgressTracker class and its methods.
"""

import pytest
from unittest.mock import patch, MagicMock
from datasync.utils.progress import ProgressTracker, track_operation

class TestProgressTracker:
    """Test suite for ProgressTracker class."""

    def test_initialization(self):
        """Test the initialization of ProgressTracker."""
        tracker = ProgressTracker(100, "Test Operation")
        assert tracker.total == 100
        assert tracker.current == 0
        assert tracker.operation == "Test Operation"
        assert tracker.use_click is True
        
    @patch('datasync.utils.progress.click.echo')
    def test_update_with_click(self, mock_echo):
        """Test update method with click output."""
        tracker = ProgressTracker(100, "Test Operation", use_click=True)
        tracker.update(10)
        
        # Assert click.echo was called
        mock_echo.assert_called_once()
        # Check the message format
        assert "Test Operation: 10 of 100" in mock_echo.call_args[0][0]
        assert "10.0%" in mock_echo.call_args[0][0]
        
        # Test with custom message
        tracker.update(10, "Custom message")
        assert "Custom message (20 of 100" in mock_echo.call_args[0][0]
        assert "20.0%" in mock_echo.call_args[0][0]
        
    @patch('builtins.print')
    def test_update_with_print(self, mock_print):
        """Test update method with print output."""
        tracker = ProgressTracker(100, "Test Operation", use_click=False)
        tracker.update(10)
        
        # Assert print was called
        mock_print.assert_called_once()
        # Check the message format
        assert "Test Operation: 10 of 100" in mock_print.call_args[0][0]
        assert "10.0%" in mock_print.call_args[0][0]
        
    @patch('datasync.utils.progress.click.echo')
    def test_complete(self, mock_echo):
        """Test complete method."""
        tracker = ProgressTracker(100, "Test Operation")
        tracker.complete()
        
        # Current should be set to total
        assert tracker.current == 100
        # Assert click.echo was called
        mock_echo.assert_called_once()
        # Check the message format
        assert "Test Operation completed (100 items processed)" in mock_echo.call_args[0][0]
        
        # Test with custom message
        tracker = ProgressTracker(100, "Test Operation")
        tracker.complete("All done!")
        assert "All done!" in mock_echo.call_args[0][0]
        
    def test_update_exceeds_total(self):
        """Test updating beyond the total."""
        tracker = ProgressTracker(100, "Test Operation", use_click=False)
        tracker.update(150)
        # Current should be capped at total
        assert tracker.current == 100
        
    @patch('datasync.utils.progress.click.echo')
    def test_track_operation(self, mock_echo):
        """Test the track_operation generator."""
        test_list = [1, 2, 3, 4, 5]
        
        # Use the generator to iterate
        result = []
        for item in track_operation(test_list, "Processing Items"):
            result.append(item)
            
        # Check that original items were yielded
        assert result == test_list
        
        # Check that progress was updated appropriately
        assert mock_echo.call_count >= 1
        
    def test_track_operation_custom_total(self):
        """Test track_operation with custom total."""
        test_list = [1, 2, 3]
        custom_total = 10
        
        with patch('datasync.utils.progress.ProgressTracker') as mock_tracker:
            instance = MagicMock()
            mock_tracker.return_value = instance
            
            for _ in track_operation(test_list, "Testing", total=custom_total):
                pass
                
            # Check the custom total was used
            mock_tracker.assert_called_once_with(custom_total, "Testing", True)
            
    def test_track_operation_no_len(self):
        """Test track_operation with an iterable that has no len()."""
        # Create a generator that has no len()
        def generator():
            for i in range(3):
                yield i
                
        with patch('datasync.utils.progress.ProgressTracker') as mock_tracker:
            instance = MagicMock()
            mock_tracker.return_value = instance
            
            for _ in track_operation(generator(), "Testing"):
                pass
                
            # Should initialize with total=0
            mock_tracker.assert_called_once_with(0, "Testing", True) 