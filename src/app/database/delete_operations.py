"""
Delete operations for Access databases.
Handles safe deletion of data with backup to temporary tables.
"""

from datetime import date
from pathlib import Path
from typing import Optional
import pyodbc
from .access_utils import access_connection, AccessDatabaseError
from .date_handling import DateFilter
from .table_operations import get_table_info

def get_temp_table_name(base_table: str, target_date: date) -> str:
    """
    Generate a temporary table name for storing deleted data.
    
    Args:
        base_table: Original table name
        target_date: Date of the deleted data
        
    Returns:
        Temporary table name in format: base_table_M_D_YYYY_temp_table
    """
    return f"{base_table}_{target_date.month}_{target_date.day}_{target_date.year}_temp_table"

def delete_data_by_date(
    db_path: Path,
    table_name: str,
    date_filter: DateFilter,
    date_column: str = "Time"
) -> str:
    """
    Delete data from a table for a specific date range and move it to a temporary table.
    
    Args:
        db_path: Path to the Access database
        table_name: Name of the table to delete from
        date_filter: DateFilter object specifying the date range
        date_column: Name of the date column to filter on (default: 'Time')
        
    Returns:
        Name of the temporary table containing the deleted data
        
    Raises:
        AccessDatabaseError: If table doesn't exist or no data found for the date
    """
    # Get table metadata to verify it exists and get column info
    table_info = get_table_info(db_path, table_name)
    
    # Generate temp table name
    temp_table = get_temp_table_name(table_name, date_filter.start_date)
    
    with access_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # First check if data exists for this date
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}] WHERE {date_filter.get_where_clause(date_column)}")
        count = cursor.fetchone()[0]
        if count == 0:
            raise AccessDatabaseError(f"No data found in table {table_name} for the specified date")
        
        # Create temp table with same structure
        columns = [f"[{col.name}] {col.data_type}" for col in table_info.columns]
        create_temp_sql = f"""
            CREATE TABLE [{temp_table}] (
                {', '.join(columns)}
            )
        """
        cursor.execute(create_temp_sql)
        
        # Copy data to temp table
        copy_sql = f"""
            INSERT INTO [{temp_table}]
            SELECT * FROM [{table_name}]
            WHERE {date_filter.get_where_clause(date_column)}
        """
        cursor.execute(copy_sql)
        
        # Delete from original table
        delete_sql = f"""
            DELETE FROM [{table_name}]
            WHERE {date_filter.get_where_clause(date_column)}
        """
        cursor.execute(delete_sql)
        
        # Verify counts match
        cursor.execute(f"SELECT COUNT(*) FROM [{temp_table}]")
        temp_count = cursor.fetchone()[0]
        if temp_count != count:
            # Rollback if counts don't match
            cursor.execute(f"DROP TABLE [{temp_table}]")
            raise AccessDatabaseError(
                f"Data integrity check failed: {count} rows selected but {temp_count} rows copied"
            )
        
        conn.commit()
        return temp_table

def cleanup_old_temp_tables(
    db_path: Path,
    days_to_keep: int = 7
) -> list[str]:
    """
    Clean up temporary tables older than the specified number of days.
    
    Args:
        db_path: Path to the Access database
        days_to_keep: Number of days to keep temp tables (default: 7)
        
    Returns:
        List of deleted temp table names
    """
    from datetime import datetime, timedelta
    
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    deleted_tables = []
    
    with access_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # Get all temp tables
        cursor.execute("""
            SELECT name FROM MSysObjects 
            WHERE type=1 AND name LIKE '%_temp_table'
        """)
        
        for (table_name,) in cursor.fetchall():
            try:
                # Extract date from table name
                date_str = table_name.split('_')[-3]  # Get M.D.YYYY part
                month, day, year = map(int, date_str.split('.'))
                table_date = datetime(year, month, day)
                
                if table_date < cutoff_date:
                    cursor.execute(f"DROP TABLE [{table_name}]")
                    deleted_tables.append(table_name)
            except (ValueError, IndexError):
                # Skip tables that don't match our naming pattern
                continue
        
        conn.commit()
        return deleted_tables 