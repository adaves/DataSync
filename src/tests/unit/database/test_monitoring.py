"""
Unit tests for database monitoring.
This module contains tests for the DatabaseMonitor class and its methods.
"""

import pytest
import time
from datetime import datetime
from datasync.database.monitoring import DatabaseMonitor, OperationMetrics

@pytest.fixture
def db_monitor():
    """Create a DatabaseMonitor instance."""
    return DatabaseMonitor()

class TestDatabaseMonitor:
    """Test suite for DatabaseMonitor class."""
    
    def test_initialization(self, db_monitor):
        """Test monitor initialization."""
        assert len(db_monitor.operation_history) == 0
        assert len(db_monitor.error_counts) == 0
        assert len(db_monitor.operation_times) == 0
    
    def test_start_operation(self, db_monitor):
        """Test starting a new operation."""
        query = "SELECT * FROM test_table"
        metrics = db_monitor.start_operation("SELECT", query)
        
        assert isinstance(metrics, OperationMetrics)
        assert metrics.operation_type == "SELECT"
        assert metrics.query == query
        assert metrics.start_time > 0
        assert metrics.end_time == 0
        assert not metrics.success
        assert metrics.affected_rows == 0
    
    def test_end_operation(self, db_monitor):
        """Test ending an operation."""
        metrics = db_monitor.start_operation("SELECT")
        time.sleep(0.1)  # Simulate some work
        db_monitor.end_operation(metrics, True, affected_rows=5)
        
        assert metrics.end_time > metrics.start_time
        assert metrics.success
        assert metrics.affected_rows == 5
        assert len(db_monitor.operation_history) == 1
        assert len(db_monitor.operation_times["SELECT"]) == 1
    
    def test_end_operation_with_error(self, db_monitor):
        """Test ending an operation with an error."""
        metrics = db_monitor.start_operation("INSERT")
        error_message = "Duplicate key error"
        db_monitor.end_operation(metrics, False, error_message)
        
        assert not metrics.success
        assert metrics.error_message == error_message
        assert db_monitor.error_counts[error_message] == 1
    
    def test_get_operation_stats(self, db_monitor):
        """Test getting operation statistics."""
        # Add some operations
        for i in range(3):
            metrics = db_monitor.start_operation("SELECT")
            db_monitor.end_operation(metrics, True, affected_rows=i+1)
        
        stats = db_monitor.get_operation_stats("SELECT")
        assert stats["total_operations"] == 3
        assert stats["successful_operations"] == 3
        assert stats["failed_operations"] == 0
        assert stats["total_rows_affected"] == 6
        assert stats["average_rows_affected"] == 2
    
    def test_get_error_summary(self, db_monitor):
        """Test getting error summary."""
        # Add some errors
        errors = [
            "Duplicate key",
            "Connection timeout",
            "Duplicate key",
            "Invalid data"
        ]
        
        for error in errors:
            metrics = db_monitor.start_operation("INSERT")
            db_monitor.end_operation(metrics, False, error)
        
        summary = db_monitor.get_error_summary()
        assert summary["Duplicate key"] == 2
        assert summary["Connection timeout"] == 1
        assert summary["Invalid data"] == 1
    
    def test_get_performance_report(self, db_monitor):
        """Test getting performance report."""
        # Add operations of different types
        operations = [
            ("SELECT", True, 10),
            ("INSERT", True, 1),
            ("SELECT", False, 0),
            ("UPDATE", True, 5)
        ]
        
        for op_type, success, rows in operations:
            metrics = db_monitor.start_operation(op_type)
            db_monitor.end_operation(
                metrics,
                success,
                "Error" if not success else None,
                rows
            )
        
        report = db_monitor.get_performance_report()
        assert "timestamp" in report
        assert report["total_operations"] == 4
        assert len(report["operation_types"]) == 3
        assert "error_summary" in report
        assert report["error_summary"]["Error"] == 1
    
    def test_clear_history(self, db_monitor):
        """Test clearing operation history."""
        # Add some operations
        metrics = db_monitor.start_operation("SELECT")
        db_monitor.end_operation(metrics, True)
        
        db_monitor.clear_history()
        assert len(db_monitor.operation_history) == 0
        assert len(db_monitor.error_counts) == 0
        assert len(db_monitor.operation_times) == 0
    
    def test_log_operation(self, db_monitor, caplog):
        """Test logging operation details."""
        metrics = db_monitor.start_operation("SELECT", "SELECT * FROM test")
        db_monitor.end_operation(metrics, True, affected_rows=5)
        
        db_monitor.log_operation(metrics)
        assert "Operation: SELECT" in caplog.text
        assert "Success: True" in caplog.text
        assert "Rows affected: 5" in caplog.text
        assert "Query: SELECT * FROM test" in caplog.text 