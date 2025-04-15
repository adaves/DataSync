"""
Database monitoring module for tracking performance, errors, and usage metrics.
This module provides functionality for monitoring database operations and
generating performance reports.
"""

from typing import Dict, Any, List, Optional
import logging
import time
from datetime import datetime
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class OperationMetrics:
    """Metrics for a single database operation."""
    operation_type: str
    start_time: float
    end_time: float
    success: bool
    error_message: Optional[str] = None
    affected_rows: int = 0
    query: Optional[str] = None

class DatabaseMonitor:
    """Handles monitoring of database operations and performance metrics."""
    
    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """
        Initialize the monitoring class.
        
        Args:
            logger: Optional logger instance for monitoring logging
        """
        self.logger = logger or logging.getLogger(__name__)
        self.operation_history: List[OperationMetrics] = []
        self.error_counts = defaultdict(int)
        self.operation_times = defaultdict(list)
    
    def start_operation(self, operation_type: str, query: Optional[str] = None) -> OperationMetrics:
        """
        Start tracking a new database operation.
        
        Args:
            operation_type: Type of operation (e.g., 'SELECT', 'INSERT', 'UPDATE')
            query: Optional SQL query being executed
            
        Returns:
            OperationMetrics object to track the operation
        """
        metrics = OperationMetrics(
            operation_type=operation_type,
            start_time=time.time(),
            end_time=0.0,
            success=False,
            query=query
        )
        return metrics
    
    def end_operation(self, metrics: OperationMetrics, success: bool, 
                     error_message: Optional[str] = None, 
                     affected_rows: int = 0) -> None:
        """
        Complete tracking of a database operation.
        
        Args:
            metrics: OperationMetrics object to update
            success: Whether the operation succeeded
            error_message: Optional error message if operation failed
            affected_rows: Number of rows affected by the operation
        """
        metrics.end_time = time.time()
        metrics.success = success
        metrics.error_message = error_message
        metrics.affected_rows = affected_rows
        
        self.operation_history.append(metrics)
        
        if not success and error_message:
            self.error_counts[error_message] += 1
        
        self.operation_times[metrics.operation_type].append(
            metrics.end_time - metrics.start_time
        )
    
    def get_operation_stats(self, operation_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics for database operations.
        
        Args:
            operation_type: Optional filter for specific operation type
            
        Returns:
            Dictionary containing operation statistics
        """
        if operation_type:
            operations = [op for op in self.operation_history 
                         if op.operation_type == operation_type]
        else:
            operations = self.operation_history
        
        if not operations:
            return {}
        
        total_time = sum(op.end_time - op.start_time for op in operations)
        success_count = sum(1 for op in operations if op.success)
        total_rows = sum(op.affected_rows for op in operations)
        
        return {
            'total_operations': len(operations),
            'successful_operations': success_count,
            'failed_operations': len(operations) - success_count,
            'total_time_seconds': total_time,
            'average_time_seconds': total_time / len(operations),
            'total_rows_affected': total_rows,
            'average_rows_affected': total_rows / len(operations) if operations else 0
        }
    
    def get_error_summary(self) -> Dict[str, int]:
        """
        Get summary of error counts by error message.
        
        Returns:
            Dictionary mapping error messages to their counts
        """
        return dict(self.error_counts)
    
    def get_performance_report(self) -> Dict[str, Any]:
        """
        Generate a comprehensive performance report.
        
        Returns:
            Dictionary containing performance metrics and statistics
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_operations': len(self.operation_history),
            'operation_types': {},
            'error_summary': self.get_error_summary()
        }
        
        # Calculate statistics for each operation type
        for op_type in set(op.operation_type for op in self.operation_history):
            report['operation_types'][op_type] = self.get_operation_stats(op_type)
        
        return report
    
    def clear_history(self) -> None:
        """Clear all stored operation history and metrics."""
        self.operation_history.clear()
        self.error_counts.clear()
        self.operation_times.clear()
    
    def log_operation(self, metrics: OperationMetrics) -> None:
        """
        Log details of a completed operation.
        
        Args:
            metrics: OperationMetrics object containing operation details
        """
        duration = metrics.end_time - metrics.start_time
        log_msg = (
            f"Operation: {metrics.operation_type}, "
            f"Duration: {duration:.3f}s, "
            f"Success: {metrics.success}, "
            f"Rows affected: {metrics.affected_rows}"
        )
        
        if metrics.query:
            log_msg += f", Query: {metrics.query}"
        
        if not metrics.success and metrics.error_message:
            log_msg += f", Error: {metrics.error_message}"
        
        if metrics.success:
            self.logger.info(log_msg)
        else:
            self.logger.error(log_msg) 