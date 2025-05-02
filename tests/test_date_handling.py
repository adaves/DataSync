"""
Tests for date input handling and SQL generation.
"""
import pytest
from datetime import datetime, date
from app.database.date_handling import parse_date_input, DateFilter, InvalidDateError

def test_parse_full_date():
    """Test parsing full date format (MM/DD/YYYY)."""
    test_cases = [
        ("1/1/2025", date(2025, 1, 1)),
        ("01/01/2025", date(2025, 1, 1)),
        ("12/31/2025", date(2025, 12, 31)),
        ("2/29/2024", date(2024, 2, 29)),  # Leap year
    ]
    
    for input_str, expected_date in test_cases:
        date_filter = parse_date_input(input_str)
        assert date_filter.start_date == expected_date
        assert date_filter.end_date == expected_date
        assert date_filter.is_full_date is True

def test_parse_year_only():
    """Test parsing year-only format (YYYY)."""
    test_cases = [
        "2025",
        " 2025 ",  # With whitespace
        "2024",    # Leap year
    ]
    
    for year_str in test_cases:
        year = int(year_str.strip())
        date_filter = parse_date_input(year_str)
        
        assert date_filter.start_date == date(year, 1, 1)
        assert date_filter.end_date == date(year, 12, 31)
        assert date_filter.is_full_date is False

def test_invalid_date_formats():
    """Test handling of invalid date formats."""
    invalid_dates = [
        "",           # Empty string
        "abc",        # Non-numeric
        "2025/01/01", # Wrong separator
        "13/01/2025", # Invalid month
        "01/32/2025", # Invalid day
        "02/29/2025", # Non-leap year February 29
        "2025-01-01", # Wrong separator
        "25",         # Incomplete year
        "202",        # Incomplete year
        None,         # None value
    ]
    
    for invalid_date in invalid_dates:
        with pytest.raises(InvalidDateError):
            parse_date_input(invalid_date)

def test_sql_where_clause():
    """Test SQL WHERE clause generation."""
    test_cases = [
        # Full date
        (
            "1/1/2025",
            "Time >= #1/1/2025# AND Time < #1/2/2025#"
        ),
        # Year only
        (
            "2025",
            "Time >= #1/1/2025# AND Time < #1/1/2026#"
        ),
        # End of month
        (
            "12/31/2025",
            "Time >= #12/31/2025# AND Time < #1/1/2026#"
        ),
        # Leap year
        (
            "2/29/2024",
            "Time >= #2/29/2024# AND Time < #3/1/2024#"
        ),
    ]
    
    for input_date, expected_where in test_cases:
        date_filter = parse_date_input(input_date)
        assert date_filter.get_where_clause("Time") == expected_where

def test_sql_parameters():
    """Test SQL parameter generation for safe queries."""
    test_cases = [
        # Full date
        (
            "1/1/2025",
            {
                "start_date": datetime(2025, 1, 1),
                "end_date": datetime(2025, 1, 2)
            }
        ),
        # Year only
        (
            "2025",
            {
                "start_date": datetime(2025, 1, 1),
                "end_date": datetime(2026, 1, 1)
            }
        ),
    ]
    
    for input_date, expected_params in test_cases:
        date_filter = parse_date_input(input_date)
        params = date_filter.get_query_parameters()
        assert params == expected_params 