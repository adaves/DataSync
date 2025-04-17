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
                        self.logger.warning(f"Failed to clean up table {table}: {e}")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
            raise

    def insert_record(self, table_name: str, record: Dict[str, Any]) -> int:
        """
        Insert a single record into a table.
        
        Args:
            table_name: Name of the table to insert into
            record: Dictionary containing column names and values
            
        Returns:
            Number of records inserted (1 if successful)
        """
        if not self.cursor:
            self.connect()
            
        try:
            # Prepare the SQL query
            columns = list(record.keys())
            placeholders = ['?' for _ in columns]
            query = f"""
                INSERT INTO [{table_name}] ({', '.join([f'[{col}]' for col in columns])})
                VALUES ({', '.join(placeholders)})
            """
            
            # Execute the query with the record values
            values = [record[col] for col in columns]
            self.cursor.execute(query, values)
            self.conn.commit()
            
            self.logger.info(f"Successfully inserted record into {table_name}")
            return 1
            
        except pyodbc.Error as e:
            self.logger.error(f"Error inserting record into {table_name}: {e}")
            self.conn.rollback()
            raise

    def batch_insert(self, table_name: str, records: List[Dict[str, Any]], 
                    batch_size: int = 1000) -> int:
        """
        Insert multiple records into a table in batches.
        
        Args:
            table_name: Name of the table to insert into
            records: List of dictionaries containing column names and values
            batch_size: Number of records to insert in each batch
            
        Returns:
            Total number of records inserted
        """
        if not records:
            return 0
            
        if not self.cursor:
            self.connect()
            
        total_inserted = 0
        temp_table = None
        
        try:
            # Create a temporary table for batch processing
            temp_table = f"temp_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            columns = list(records[0].keys())
            
            # Create temporary table with same structure
            create_temp_query = f"""
                SELECT TOP 0 *
                INTO [{temp_table}]
                FROM [{table_name}]
            """
            self.execute_query(create_temp_query)
            
            # Process records in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                if not batch:
                    continue
                    
                # Prepare batch insert query
                columns = list(batch[0].keys())
                placeholders = ['?' for _ in columns]
                query = f"""
                    INSERT INTO [{temp_table}] ({', '.join([f'[{col}]' for col in columns])})
                    VALUES ({', '.join(placeholders)})
                """
                
                # Execute batch insert
                values = [[record[col] for col in columns] for record in batch]
                self.cursor.executemany(query, values)
                total_inserted += len(batch)
                
                self.logger.info(f"Inserted batch of {len(batch)} records into temporary table")
            
            # Copy from temporary table to target table
            copy_query = f"""
                INSERT INTO [{table_name}]
                SELECT * FROM [{temp_table}]
            """
            self.execute_query(copy_query)
            self.conn.commit()
            
            self.logger.info(f"Successfully inserted {total_inserted} records into {table_name}")
            return total_inserted
            
        except pyodbc.Error as e:
            self.logger.error(f"Error during batch insert into {table_name}: {e}")
            self.conn.rollback()
            raise
            
        finally:
            # Clean up temporary table
            if temp_table:
                try:
                    self.execute_query(f"DROP TABLE [{temp_table}]")
                except Exception as e:
                    self.logger.warning(f"Failed to clean up temporary table: {e}")

    def upsert(self, table_name: str, record: Dict[str, Any], 
              key_columns: List[str]) -> int:
        """
        Update a record if it exists, otherwise insert it.
        
        Args:
            table_name: Name of the table
            record: Dictionary containing column names and values
            key_columns: List of column names that form the unique key
            
        Returns:
            Number of records affected (1 if successful)
        """
        if not self.cursor:
            self.connect()
            
        try:
            # Build WHERE clause for key columns
            where_clause = ' AND '.join([f'[{col}] = ?' for col in key_columns])
            key_values = [record[col] for col in key_columns]
            
            # Check if record exists
            check_query = f"""
                SELECT COUNT(*) as count
                FROM [{table_name}]
                WHERE {where_clause}
            """
            result = self.execute_query(check_query, key_values)
            exists = result[0]['count'] > 0 if result else False
            
            if exists:
                # Update existing record
                update_cols = [col for col in record.keys() if col not in key_columns]
                set_clause = ', '.join([f'[{col}] = ?' for col in update_cols])
                update_values = [record[col] for col in update_cols] + key_values
                
                update_query = f"""
                    UPDATE [{table_name}]
                    SET {set_clause}
                    WHERE {where_clause}
                """
                self.cursor.execute(update_query, update_values)
                self.logger.info(f"Updated existing record in {table_name}")
            else:
                # Insert new record
                return self.insert_record(table_name, record)
                
            self.conn.commit()
            return 1
            
        except pyodbc.Error as e:
            self.logger.error(f"Error during upsert into {table_name}: {e}")
            self.conn.rollback()
            raise

    def begin_transaction(self) -> None:
        """Begin a new transaction."""
        if not self.cursor:
            self.connect()
        self.conn.autocommit = False
        self.logger.info("Transaction started")

    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if self.conn:
            self.conn.commit()
            self.conn.autocommit = True
            self.logger.info("Transaction committed")

    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if self.conn:
            self.conn.rollback()
            self.conn.autocommit = True
            self.logger.info("Transaction rolled back") 