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
import json
import os

@dataclass
class OperationMetrics:
    """Metrics for a single database operation."""
    operation_name: str
    start_time: float
    end_time: float
    duration: float
    error_count: int = 0
    last_error: Optional[str] = None
    exceeded_threshold: bool = False
    threshold_duration: Optional[float] = None
    timed_out: bool = False
    timeout_duration: Optional[float] = None
    category: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class AggregatedMetrics:
    """Aggregated metrics for a set of operations."""
    operation_count: int
    average_duration: float
    total_duration: float
    error_count: int
    category: Optional[str] = None

class DatabaseMonitor:
    """Handles monitoring of database operations and performance metrics."""
    
    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """
        Initialize the monitoring class.
        
        Args:
            logger: Optional logger instance for monitoring logging
        """
        self.logger = logger or logging.getLogger(__name__)
        self.operations: Dict[str, OperationMetrics] = {}
        self.performance_thresholds: Dict[str, float] = {}
        self.operation_timeouts: Dict[str, float] = {}
        self.metrics_file = "metrics.json"
    
    def start_operation(self, operation_name: str, category: Optional[str] = None,
                       metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Start tracking a new database operation.
        
        Args:
            operation_name: Name of the operation
            category: Optional category for the operation
            metadata: Optional additional metadata
            
        Returns:
            Operation ID
        """
        operation_id = f"{operation_name}_{len(self.operations)}"
        self.operations[operation_id] = OperationMetrics(
            operation_name=operation_name,
            start_time=time.time(),
            end_time=0.0,
            duration=0.0,
            category=category,
            metadata=metadata
        )
        return operation_id
    
    def end_operation(self, operation_id: str) -> OperationMetrics:
        """
        Complete tracking of a database operation.
        
        Args:
            operation_id: ID of the operation to end
            
        Returns:
            OperationMetrics object
        """
        if operation_id not in self.operations:
            raise ValueError(f"Unknown operation ID: {operation_id}")
        
        metrics = self.operations[operation_id]
        metrics.end_time = time.time()
        metrics.duration = metrics.end_time - metrics.start_time
        
        # Check performance threshold
        if metrics.operation_name in self.performance_thresholds:
            threshold = self.performance_thresholds[metrics.operation_name]
            if metrics.duration > threshold:
                metrics.exceeded_threshold = True
                metrics.threshold_duration = threshold
        
        # Check timeout
        if metrics.operation_name in self.operation_timeouts:
            timeout = self.operation_timeouts[metrics.operation_name]
            if metrics.duration > timeout:
                metrics.timed_out = True
                metrics.timeout_duration = timeout
        
        return metrics
    
    def record_error(self, operation_id: str, error: Exception) -> None:
        """
        Record an error for an operation.
        
        Args:
            operation_id: ID of the operation
            error: Exception that occurred
        """
        if operation_id not in self.operations:
            raise ValueError(f"Unknown operation ID: {operation_id}")
        
        metrics = self.operations[operation_id]
        metrics.error_count += 1
        metrics.last_error = str(error)
    
    def set_performance_threshold(self, operation_name: str, threshold: float) -> None:
        """
        Set performance threshold for an operation.
        
        Args:
            operation_name: Name of the operation
            threshold: Threshold in seconds
        """
        self.performance_thresholds[operation_name] = threshold
    
    def set_operation_timeout(self, operation_name: str, timeout: float) -> None:
        """
        Set operation timeout.
        
        Args:
            operation_name: Name of the operation
            timeout: Timeout in seconds
        """
        self.operation_timeouts[operation_name] = timeout
    
    def get_concurrent_operation_count(self) -> int:
        """
        Get number of concurrent operations.
        
        Returns:
            Number of concurrent operations
        """
        return sum(1 for metrics in self.operations.values() 
                  if metrics.end_time == 0.0)
    
    def get_aggregated_metrics(self, operation_name: str) -> AggregatedMetrics:
        """
        Get aggregated metrics for an operation type.
        
        Args:
            operation_name: Name of the operation
            
        Returns:
            AggregatedMetrics object
        """
        operations = [m for m in self.operations.values() 
                     if m.operation_name == operation_name]
        
        if not operations:
            return AggregatedMetrics(0, 0.0, 0.0, 0)
        
        total_duration = sum(m.duration for m in operations)
        error_count = sum(m.error_count for m in operations)
        
        return AggregatedMetrics(
            operation_count=len(operations),
            average_duration=total_duration / len(operations),
            total_duration=total_duration,
            error_count=error_count
        )
    
    def get_category_metrics(self, category: str) -> AggregatedMetrics:
        """
        Get aggregated metrics for a category.
        
        Args:
            category: Category name
            
        Returns:
            AggregatedMetrics object
        """
        operations = [m for m in self.operations.values() 
                     if m.category == category]
        
        if not operations:
            return AggregatedMetrics(0, 0.0, 0.0, 0, category)
        
        total_duration = sum(m.duration for m in operations)
        error_count = sum(m.error_count for m in operations)
        
        return AggregatedMetrics(
            operation_count=len(operations),
            average_duration=total_duration / len(operations),
            total_duration=total_duration,
            error_count=error_count,
            category=category
        )
    
    def save_metrics(self) -> None:
        """Save current metrics to file."""
        metrics_data = {
            "operations": {
                op_id: {
                    "operation_name": metrics.operation_name,
                    "start_time": metrics.start_time,
                    "end_time": metrics.end_time,
                    "duration": metrics.duration,
                    "error_count": metrics.error_count,
                    "last_error": metrics.last_error,
                    "exceeded_threshold": metrics.exceeded_threshold,
                    "threshold_duration": metrics.threshold_duration,
                    "timed_out": metrics.timed_out,
                    "timeout_duration": metrics.timeout_duration,
                    "category": metrics.category,
                    "metadata": metrics.metadata
                }
                for op_id, metrics in self.operations.items()
            },
            "performance_thresholds": self.performance_thresholds,
            "operation_timeouts": self.operation_timeouts
        }
        
        with open(self.metrics_file, 'w') as f:
            json.dump(metrics_data, f)
    
    def load_metrics(self) -> None:
        """Load metrics from file."""
        if not os.path.exists(self.metrics_file):
            return
        
        with open(self.metrics_file, 'r') as f:
            metrics_data = json.load(f)
        
        self.operations = {
            op_id: OperationMetrics(**metrics)
            for op_id, metrics in metrics_data["operations"].items()
        }
        self.performance_thresholds = metrics_data["performance_thresholds"]
        self.operation_timeouts = metrics_data["operation_timeouts"]
    
    def clear_metrics(self) -> None:
        """Clear current metrics."""
        self.operations.clear()
        self.performance_thresholds.clear()
        self.operation_timeouts.clear()
    
    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self.clear_metrics()
        if os.path.exists(self.metrics_file):
            os.remove(self.metrics_file) 