"""
Core database operations for Microsoft Access database interactions.
This module handles all direct database operations including connection management,
CRUD operations, and table management.
"""

import pyodbc
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import logging

class DatabaseOperations:
    """Handles core database operations for Microsoft Access databases."""
    
    def __init__(self, db_path: Union[str, Path]) -> None:
        """
        Initialize database connection parameters.
        
        Args:
            db_path: Path to the Access database file
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        # Create connection string with proper path handling
        absolute_path = str(self.db_path.absolute())
        # Ensure the path is properly escaped for ODBC
        absolute_path = absolute_path.replace('\\', '\\\\')
        
        self.conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            f'DBQ={absolute_path};'
        )
        
        # Initialize connection attributes
        self.conn = None
        self.cursor = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> None:
        """Establish connection to the database."""
        try:
            self.conn = pyodbc.connect(self.conn_str)
            self.cursor = self.conn.cursor()
            self.logger.info("Successfully connected to the database")
        except pyodbc.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            self.logger.error(f"Connection string used: {self.conn_str}")
            raise
    
    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            self.logger.info("Database connection closed")
    
    def get_tables(self) -> List[str]:
        """
        Get list of all tables in the database.
        
        Returns:
            List of table names
        """
        if not self.cursor:
            self.connect()
        
        tables = []
        for row in self.cursor.tables():
            if row.table_type == 'TABLE':
                tables.append(row.table_name)
        return tables
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Union[List[Dict[str, Any]], int]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute
            params: Optional parameters for the query
            
        Returns:
            For SELECT queries: List of dictionaries containing results
            For other queries: Number of affected rows
        """
        if not self.cursor:
            self.connect()
        
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            # If it's a SELECT query, fetch results
            if query.strip().upper().startswith('SELECT'):
                columns = [column[0] for column in self.cursor.description]
                results = []
                for row in self.cursor.fetchall():
                    # Convert row to dictionary using column names
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[columns[i]] = value
                    results.append(row_dict)
                return results
            else:
                self.conn.commit()
                return self.cursor.rowcount
        except pyodbc.Error as e:
            self.logger.error(f"Error executing query: {e}")
            raise
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get the column names from a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column names
        """
        try:
            query = f"SELECT * FROM [{table_name}] WHERE 1=0"  # Get structure without data
            self.cursor.execute(query)
            columns = [column[0] for column in self.cursor.description]
            return columns
        except Exception as e:
            self.logger.error(f"Error getting table columns: {e}")
            raise
    
    def read_table(self, table_name: str) -> pd.DataFrame:
        """
        Read an entire table into a pandas DataFrame.
        
        Args:
            table_name: Name of the table to read
            
        Returns:
            DataFrame containing table data
        """
        if not self.cursor:
            self.connect()
        
        try:
            query = f"SELECT * FROM [{table_name}]"
            return pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(f"Error reading table {table_name}: {e}")
            raise
    
    def count_records(self, table_name: str, year: int, date_column: str = 'Time') -> int:
        """
        Count records for a specific year in a table.
        
        Args:
            table_name: Name of the table
            year: Year to count records for
            date_column: Name of the date column
            
        Returns:
            Number of records found
        """
        try:
            query = f"SELECT COUNT(*) as record_count FROM [{table_name}] WHERE YEAR([{date_column}]) = ?"
            result = self.execute_query(query, [year])
            return result[0]['record_count'] if result else 0
        except Exception as e:
            self.logger.error(f"Error counting records: {e}")
            raise
    
    def delete_year_data(self, table_name: str, year: int, date_column: str = 'Time') -> int:
        """
        Delete all records for a specific year from a table.
        
        Args:
            table_name: Name of the table
            year: Year to delete records for
            date_column: Name of the date column
            
        Returns:
            Number of records deleted
        """
        try:
            # First count the records to be deleted
            record_count = self.count_records(table_name, year, date_column)
            
            if record_count == 0:
                self.logger.info(f"No records found for year {year}")
                return 0
            
            self.logger.info(f"Starting deletion of {record_count:,} records")
            
            # Define the quarterly dates
            quarterly_dates = [
                f"01/01/{year}",  # Q1
                f"04/01/{year}",  # Q2
                f"07/01/{year}",  # Q3
                f"10/01/{year}"   # Q4
            ]
            
            total_deleted = 0
            
            # Process one quarter at a time
            for quarter, date in enumerate(quarterly_dates, 1):
                self.logger.info(f"Processing Q{quarter} ({date})")
                
                try:
                    query = f"""
                        DELETE FROM [{table_name}]
                        WHERE [{date_column}] = #{date}#
                    """
                    
                    count = self.execute_query(query)
                    total_deleted += count
                    self.logger.info(f"Q{quarter}: Deleted {count} records")
                    
                except Exception as e:
                    self.logger.error(f"Error deleting data for Q{quarter}: {e}")
                    self.conn.rollback()
                    continue
            
            # Final verification
            remaining = self.count_records(table_name, year, date_column)
            if remaining == 0:
                self.logger.info(f"Verification successful: No records remain for year {year}")
                self.logger.info(f"Total records deleted: {total_deleted:,}")
            else:
                self.logger.warning(f"Verification failed: {remaining} records still exist for year {year}")
            
            return total_deleted
            
        except Exception as e:
            self.logger.error(f"Error deleting data for year {year}: {e}")
            raise
    
    def cleanup_temp_tables(self) -> None:
        """Clean up any leftover temporary tables."""
        try:
            tables = self.get_tables()
            for table in tables:
                if table.startswith('temp_batch_'):
                    try:
                        self.execute_query(f"DROP TABLE [{table}]")
                        self.logger.info(f"Cleaned up temporary table: {table}")
                    except Exception as e:
                        self.logger.warning(f"Failed to clean up temporary table {table}: {e}")
        except Exception as e:
            self.logger.warning(f"Error during temporary table cleanup: {e}") 