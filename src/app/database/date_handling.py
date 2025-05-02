"""
Date handling utilities for Access database operations.
Handles date parsing, validation, and SQL generation.
"""

from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Dict, Optional
import re

class InvalidDateError(Exception):
    """Raised when date input cannot be parsed or is invalid."""
    pass

@dataclass(frozen=True)
class DateFilter:
    """
    Represents a date filter for database queries.
    For full dates, start_date equals end_date.
    For year-only, start_date is Jan 1 and end_date is Dec 31.
    """
    start_date: date
    end_date: date
    is_full_date: bool

    def get_where_clause(self, column_name: str) -> str:
        """
        Generate SQL WHERE clause for Access database.
        Uses half-open interval [start, end) for consistent filtering.
        
        Args:
            column_name: Name of the date column to filter on
            
        Returns:
            SQL WHERE clause string using Access date literals
        """
        next_day = self.end_date + timedelta(days=1) if self.is_full_date else date(self.end_date.year + 1, 1, 1)
        
        return (
            f"{column_name} >= #{self.start_date.month}/{self.start_date.day}/{self.start_date.year}# AND "
            f"{column_name} < #{next_day.month}/{next_day.day}/{next_day.year}#"
        )

    def get_query_parameters(self) -> Dict[str, datetime]:
        """
        Get parameters for parameterized queries.
        Uses datetime objects as required by pyodbc.
        
        Returns:
            Dictionary with start_date and end_date parameters
        """
        next_day = self.end_date + timedelta(days=1) if self.is_full_date else date(self.end_date.year + 1, 1, 1)
        
        return {
            "start_date": datetime.combine(self.start_date, datetime.min.time()),
            "end_date": datetime.combine(next_day, datetime.min.time())
        }

def parse_date_input(date_str: Optional[str]) -> DateFilter:
    """
    Parse date input string into a DateFilter object.
    
    Args:
        date_str: Date string in either MM/DD/YYYY or YYYY format
        
    Returns:
        DateFilter object with parsed dates
        
    Raises:
        InvalidDateError: If date string is invalid or cannot be parsed
    """
    if not date_str:
        raise InvalidDateError("Date string cannot be empty or None")
    
    date_str = date_str.strip()
    
    # Try year-only format first (YYYY)
    year_match = re.match(r'^\d{4}$', date_str)
    if year_match:
        year = int(date_str)
        try:
            return DateFilter(
                start_date=date(year, 1, 1),
                end_date=date(year, 12, 31),
                is_full_date=False
            )
        except ValueError as e:
            raise InvalidDateError(f"Invalid year: {e}")
    
    # Try full date format (MM/DD/YYYY)
    date_match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', date_str)
    if not date_match:
        raise InvalidDateError(
            "Invalid date format. Use MM/DD/YYYY for full date or YYYY for year only."
        )
    
    try:
        month, day, year = map(int, date_match.groups())
        parsed_date = date(year, month, day)
        return DateFilter(
            start_date=parsed_date,
            end_date=parsed_date,
            is_full_date=True
        )
    except ValueError as e:
        raise InvalidDateError(f"Invalid date: {e}") 