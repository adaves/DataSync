"""
Unit tests for database monitoring.
This module contains tests for the DatabaseMonitor class and its methods.
"""

import pytest
import time
from datetime import datetime
from datasync.database.monitoring import DatabaseMonitor, OperationMetrics
from tests.fixtures.database import db_monitor

class TestDatabaseMonitor:
    """Test suite for DatabaseMonitor class."""
    
    def test_operation_tracking(self, db_monitor):
        """Test operation tracking functionality."""
        # Start tracking an operation
        operation_id = db_monitor.start_operation("test_operation")
        assert operation_id is not None
        
        # Simulate some work
        time.sleep(0.1)
        
        # End the operation
        metrics = db_monitor.end_operation(operation_id)
        assert isinstance(metrics, OperationMetrics)
        assert metrics.operation_name == "test_operation"
        assert metrics.duration > 0
        assert metrics.start_time < metrics.end_time
    
    def test_error_tracking(self, db_monitor):
        """Test error tracking functionality."""
        # Start tracking an operation
        operation_id = db_monitor.start_operation("test_operation")
        
        # Record an error
        error = Exception("Test error")
        db_monitor.record_error(operation_id, error)
        
        # End the operation
        metrics = db_monitor.end_operation(operation_id)
        assert metrics.error_count == 1
        assert metrics.last_error == str(error)
    
    def test_metrics_aggregation(self, db_monitor):
        """Test metrics aggregation functionality."""
        # Track multiple operations
        operation_ids = []
        for i in range(3):
            operation_id = db_monitor.start_operation(f"test_operation_{i}")
            time.sleep(0.1)
            db_monitor.end_operation(operation_id)
            operation_ids.append(operation_id)
        
        # Get aggregated metrics
        aggregated_metrics = db_monitor.get_aggregated_metrics("test_operation")
        assert aggregated_metrics.operation_count == 3
        assert aggregated_metrics.average_duration > 0
        assert aggregated_metrics.total_duration > 0
    
    def test_performance_thresholds(self, db_monitor):
        """Test performance threshold monitoring."""
        # Set a performance threshold
        db_monitor.set_performance_threshold("test_operation", 0.2)  # 200ms
        
        # Track a slow operation
        operation_id = db_monitor.start_operation("test_operation")
        time.sleep(0.3)  # Exceeds threshold
        metrics = db_monitor.end_operation(operation_id)
        
        assert metrics.exceeded_threshold is True
        assert metrics.threshold_duration == 0.2
    
    def test_operation_categories(self, db_monitor):
        """Test operation categorization."""
        # Track operations in different categories
        categories = ["read", "write", "delete"]
        for category in categories:
            operation_id = db_monitor.start_operation(f"test_{category}", category=category)
            time.sleep(0.1)
            db_monitor.end_operation(operation_id)
        
        # Verify category metrics
        for category in categories:
            metrics = db_monitor.get_category_metrics(category)
            assert metrics.operation_count == 1
            assert metrics.category == category
    
    def test_metrics_persistence(self, db_monitor):
        """Test metrics persistence functionality."""
        # Track some operations
        operation_ids = []
        for i in range(3):
            operation_id = db_monitor.start_operation(f"test_operation_{i}")
            time.sleep(0.1)
            db_monitor.end_operation(operation_id)
            operation_ids.append(operation_id)
        
        # Save metrics
        db_monitor.save_metrics()
        
        # Clear current metrics
        db_monitor.clear_metrics()
        
        # Load saved metrics
        db_monitor.load_metrics()
        
        # Verify metrics were restored
        aggregated_metrics = db_monitor.get_aggregated_metrics("test_operation")
        assert aggregated_metrics.operation_count == 3
    
    def test_operation_metadata(self, db_monitor):
        """Test operation metadata tracking."""
        # Start operation with metadata
        metadata = {
            "table": "test_table",
            "record_count": 100,
            "batch_size": 50
        }
        operation_id = db_monitor.start_operation("test_operation", metadata=metadata)
        
        # End operation
        metrics = db_monitor.end_operation(operation_id)
        
        # Verify metadata
        assert metrics.metadata == metadata
    
    def test_concurrent_operations(self, db_monitor):
        """Test tracking of concurrent operations."""
        # Start multiple concurrent operations
        operation_ids = []
        for i in range(3):
            operation_id = db_monitor.start_operation(f"concurrent_operation_{i}")
            operation_ids.append(operation_id)
        
        # Verify concurrent operation count
        assert db_monitor.get_concurrent_operation_count() == 3
        
        # End operations
        for operation_id in operation_ids:
            db_monitor.end_operation(operation_id)
        
        # Verify no concurrent operations
        assert db_monitor.get_concurrent_operation_count() == 0
    
    def test_operation_timeout(self, db_monitor):
        """Test operation timeout monitoring."""
        # Set operation timeout
        db_monitor.set_operation_timeout("test_operation", 0.2)  # 200ms
        
        # Start operation
        operation_id = db_monitor.start_operation("test_operation")
        
        # Simulate timeout
        time.sleep(0.3)
        
        # End operation
        metrics = db_monitor.end_operation(operation_id)
        
        assert metrics.timed_out is True
        assert metrics.timeout_duration == 0.2
    
    def test_metrics_reset(self, db_monitor):
        """Test metrics reset functionality."""
        # Track some operations
        for i in range(3):
            operation_id = db_monitor.start_operation(f"test_operation_{i}")
            time.sleep(0.1)
            db_monitor.end_operation(operation_id)
        
        # Reset metrics
        db_monitor.reset_metrics()
        
        # Verify metrics are cleared
        aggregated_metrics = db_monitor.get_aggregated_metrics("test_operation")
        assert aggregated_metrics.operation_count == 0
        assert aggregated_metrics.total_duration == 0 