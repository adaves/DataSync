"""
Core database operations for Microsoft Access database interactions.
This module handles all direct database operations including connection management,
CRUD operations, and table management.
"""

import os
import pyodbc
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import logging
from datasync.database.exceptions import DatabaseError
from datasync.database.transaction import TransactionManager
from datasync.utils.logger import setup_logger
from datasync.utils.mock_database import create_mock_database
from ..utils.error_handling import handle_database_error
from ..utils.path_utils import normalize_path

class DatabaseOperations:
    """Handles core database operations for Microsoft Access databases."""
    
    def __init__(self, db_path: Union[str, Path]) -> None:
        """
        Initialize database connection parameters.
        
        Args:
            db_path: Path to the Access database file
        """
        self.db_path = normalize_path(db_path)
        if not os.path.exists(os.path.dirname(self.db_path)):
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
        # Initialize connection attributes
        self.conn = None
        self.cursor = None
        self.logger = logging.getLogger(__name__)
        self.transaction_manager = None
        self._is_connected = False
    
    @property
    def conn_str(self) -> str:
        """
        Generate the connection string for the database.
        
        Returns:
            str: The formatted connection string
        """
        db_path = str(self.db_path.absolute()).replace("\\", "\\\\")
        return (
            "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={db_path};"
            "ExtendedAnsiSQL=1;"
            "UID=Admin;"
            "PWD=;"
        )
    
    @property
    def is_connected(self) -> bool:
        """Check if the database is currently connected."""
        return self._is_connected and self.conn is not None and self.cursor is not None
    
    def connect(self) -> None:
        """Connect to the database."""
        if self.is_connected:
            return

        try:
            # Create database if it doesn't exist
            if not os.path.exists(self.db_path):
                create_mock_database(self.db_path)
            
            self.conn = pyodbc.connect(self.conn_str)
            self.cursor = self.conn.cursor()
            self.transaction_manager = TransactionManager(self)
            self._is_connected = True
            self.logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            self._is_connected = False
            self.conn = None
            self.cursor = None
            self.logger.error(f"Failed to connect to database: {str(e)}")
            raise DatabaseError(f"Failed to connect to database: {str(e)}")
    
    def close(self) -> None:
        """Close the database connection."""
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.conn:
                if not self.in_transaction:  # Only commit if not in a transaction
                    self.conn.commit()
                self.conn.close()
                self.conn = None
                self.transaction_manager = None
                self._is_connected = False
                self.logger.info("Database connection closed")
        except Exception as e:
            self.logger.error(f"Error closing database connection: {str(e)}")
            raise DatabaseError(f"Error closing database connection: {str(e)}")
    
    @property
    def in_transaction(self) -> bool:
        """Check if currently in a transaction."""
        return self.transaction_manager.in_transaction if self.transaction_manager else False
    
    def begin_transaction(self) -> None:
        """Begin a new transaction."""
        if not self.is_connected:
            self.connect()
        if not self.in_transaction:
            self.transaction_manager.begin()
            self.logger.debug("Transaction started")
    
    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if self.in_transaction:
            self.transaction_manager.commit()
            self.logger.debug("Transaction committed")
    
    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if self.in_transaction:
            self.transaction_manager.rollback()
            self.logger.debug("Transaction rolled back")
    
    def get_tables(self) -> List[str]:
        """
        Get a list of all tables in the database.
        
        Returns:
            List of table names
        """
        if not self.cursor:
            self.connect()
            
        query = """
            SELECT Name 
            FROM MSysObjects 
            WHERE Type=1 AND Flags=0
        """
        results = self.execute_query(query)
        return [row['Name'] for row in results]

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get a list of column names for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column names
        """
        if not self.cursor:
            self.connect()
            
        query = f"SELECT TOP 1 * FROM [{table_name}]"
        self.cursor.execute(query)
        return [column[0] for column in self.cursor.description]

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if the table exists, False otherwise
        """
        return table_name in self.get_tables()

    def drop_table(self, table_name: str) -> None:
        """
        Drop a table from the database.
        
        Args:
            table_name: Name of the table to drop
        """
        if not self.cursor:
            self.connect()
            
        if not self.table_exists(table_name):
            raise DatabaseError(f"Table '{table_name}' does not exist")
            
        query = f"DROP TABLE [{table_name}]"
        self.execute_query(query)
        if not self.in_transaction:
            self.conn.commit()

    def truncate_table(self, table_name: str) -> None:
        """
        Remove all records from a table.
        
        Args:
            table_name: Name of the table to truncate
        """
        if not self.cursor:
            self.connect()
            
        if not self.table_exists(table_name):
            raise DatabaseError(f"Table '{table_name}' does not exist")
            
        query = f"DELETE FROM [{table_name}]"
        self.execute_query(query)
        if not self.in_transaction:
            self.conn.commit()

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table information
        """
        if not self.cursor:
            self.connect()
            
        if not self.table_exists(table_name):
            raise DatabaseError(f"Table '{table_name}' does not exist")
            
        # Get column information
        columns = []
        self.cursor.execute(f"SELECT TOP 1 * FROM [{table_name}]")
        for column in self.cursor.description:
            columns.append({
                'name': column[0],
                'type': column[1].__name__,
                'nullable': bool(column[6]),
                'size': column[3]
            })
            
        # Get record count
        count_query = f"SELECT COUNT(*) as count FROM [{table_name}]"
        count_result = self.execute_query(count_query)
        record_count = count_result[0]['count']
        
        return {
            'name': table_name,
            'columns': columns,
            'record_count': record_count
        }

    def verify_record_exists(self, table_name: str, conditions: Dict[str, Any]) -> bool:
        """
        Verify that a record exists in the table.
        
        Args:
            table_name: Name of the table
            conditions: Dictionary of column names and values to check
            
        Returns:
            True if the record exists, False otherwise
        """
        if not self.cursor:
            self.connect()
            
        where_clause = ' AND '.join([f'[{col}] = ?' for col in conditions.keys()])
        values = list(conditions.values())
        
        query = f"""
            SELECT COUNT(*) as count 
            FROM [{table_name}] 
            WHERE {where_clause}
        """
        
        result = self.execute_query(query, values)
        return result[0]['count'] > 0

    def execute_query(self, query: str, params: Optional[Union[tuple, List[Any]]] = None) -> Any:
        """
        Execute a SQL query and return the results.
        
        Args:
            query: SQL query to execute
            params: Optional parameters for the query
            
        Returns:
            Query results (list of dictionaries for SELECT queries, row count for others)
        """
        if not self.cursor:
            self.connect()
            
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
                
            # For SELECT queries, return results as list of dictionaries
            if query.strip().upper().startswith('SELECT'):
                columns = [column[0] for column in self.cursor.description]
                return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            
            # For other queries (INSERT, UPDATE, DELETE), return affected row count
            return self.cursor.rowcount
            
        except Exception as e:
            if self.in_transaction:
                self.rollback_transaction()
            raise DatabaseError(f"Query execution failed: {str(e)}")

    def __enter__(self) -> 'DatabaseOperations':
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

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
    
    @handle_database_error
    def record_exists(self, table_name: str, conditions: Dict[str, Any]) -> bool:
        """
        Check if a record exists in the table.
        
        Args:
            table_name: Name of the table to check
            conditions: Dictionary of column names and values to match
            
        Returns:
            True if the record exists, False otherwise
        """
        where_clause = " AND ".join([f"[{k}] = ?" for k in conditions.keys()])
        query = f"SELECT COUNT(*) as count FROM [{table_name}] WHERE {where_clause}"
        
        result = self.execute_query(query, list(conditions.values()))
        return result[0]['count'] > 0

    @handle_database_error
    def verify_record_exists(self, table_name: str, conditions: Dict[str, Any]) -> None:
        """
        Verify that a record exists, raising an error if it doesn't.
        
        Args:
            table_name: Name of the table to check
            conditions: Dictionary of column names and values to match
            
        Raises:
            DatabaseError: If the record doesn't exist
        """
        if not self.record_exists(table_name, conditions):
            conditions_str = ", ".join(f"{k}={v}" for k, v in conditions.items())
            raise DatabaseError(
                f"Record not found in table '{table_name}' with conditions: {conditions_str}",
                details={
                    'table': table_name,
                    'conditions': conditions
                }
            )

    @handle_database_error
    def count_records(self, table_name: str, year: int, date_column: str = 'date_field') -> int:
        """
        Count records for a specific year in a table.
        
        Args:
            table_name: Name of the table
            year: Year to count records for
            date_column: Name of the date column (defaults to 'date_field')
            
        Returns:
            Number of records found
        """
        if not self.cursor:
            self.connect()
            
        # Verify table exists
        if table_name not in self.get_tables():
            raise DatabaseError(f"Table '{table_name}' does not exist")
            
        # Verify column exists
        columns = self.get_table_columns(table_name)
        if date_column not in columns:
            raise DatabaseError(f"Column '{date_column}' not found in table '{table_name}'")
            
        query = f"""
            SELECT COUNT(*) as record_count 
            FROM [{table_name}] 
            WHERE YEAR([{date_column}]) = ?
        """
        
        result = self.execute_query(query, (year,))
        return result[0]['record_count']

    @handle_database_error
    def delete_year_data(self, table_name: str, year: int, 
                        date_column: str = 'date_field',
                        batch_size: int = 1000) -> int:
        """
        Delete all records for a specific year from a table.
        
        Args:
            table_name: Name of the table to delete from
            year: Year to delete records for
            date_column: Name of the date column (defaults to 'date_field')
            batch_size: Number of records to delete in each batch
            
        Returns:
            Number of records deleted
        """
        # First count the records
        count = self.count_records(table_name, year, date_column)
        if count == 0:
            raise ValueError(f"No records found for year {year} in table {table_name}")
            
        self.logger.info(f"Starting batch deletion of {count:,} records")
        total_deleted = 0
        
        # Process in batches to avoid lock count issues
        while total_deleted < count:
            try:
                # Begin transaction for this batch
                self.begin_transaction()
                
                # Delete records in batches
                query = f"""
                    DELETE TOP {batch_size} 
                    FROM [{table_name}]
                    WHERE YEAR([{date_column}]) = ?
                """
                
                # Execute the delete
                deleted = self.execute_query(query, (year,))
                if deleted == 0:
                    break  # No more records to delete
                    
                total_deleted += deleted
                self.logger.info(f"Deleted batch of {deleted} records (total: {total_deleted:,})")
                
                # Commit this batch
                self.commit_transaction()
                
            except Exception as e:
                self.rollback_transaction()
                self.logger.error(f"Error deleting batch: {str(e)}")
                raise
                
        # Final verification
        remaining = self.count_records(table_name, year, date_column)
        if remaining > 0:
            raise RuntimeError(
                f"Deletion incomplete: {remaining} records still exist for year {year}"
            )
            
        self.logger.info(f"Successfully deleted {total_deleted:,} records for year {year}")
        return total_deleted

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

    @handle_database_error
    def insert_data(self, table: str, data: Dict[str, Any]) -> int:
        """
        Insert data into a table.
        
        Args:
            table: Table name
            data: Dictionary of column names and values
            
        Returns:
            ID of the inserted row
        """
        columns = ', '.join(f"[{col}]" for col in data.keys())
        placeholders = ', '.join(['?' for _ in data])
        query = f"INSERT INTO [{table}] ({columns}) VALUES ({placeholders})"
        
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, list(data.values()))
            return cursor.lastrowid
        finally:
            cursor.close()

    @handle_database_error
    def insert_record(self, table_name: str, record: Dict[str, Any]) -> int:
        """
        Insert a single record into the specified table.
        
        Args:
            table_name: Name of the table to insert into
            record: Dictionary of column names and values
            
        Returns:
            int: The ID of the inserted record
        """
        if not self.is_connected:
            self.connect()
            
        try:
            # Convert data types to Access-compatible formats
            for key, value in record.items():
                if isinstance(value, datetime):
                    record[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(value, bool):
                    record[key] = -1 if value else 0
                elif value is None:
                    record[key] = None
                    
            columns = ', '.join(record.keys())
            placeholders = ', '.join(['?' for _ in record])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            self.cursor.execute(query, list(record.values()))
            self.cursor.execute("SELECT @@IDENTITY")
            record_id = self.cursor.fetchone()[0]
            
            if not self.in_transaction:
                self.conn.commit()
                
            return record_id
        except Exception as e:
            if self.in_transaction:
                self.rollback_transaction()
            raise DatabaseError(f"Failed to insert record: {str(e)}")
    
    @handle_database_error
    def update_record(self, table_name: str, record: Dict[str, Any], 
                     condition: Optional[str] = None) -> int:
        """
        Update records in the specified table.
        
        Args:
            table_name: Name of the table to update
            record: Dictionary of column names and new values
            condition: Optional WHERE clause condition
            
        Returns:
            int: Number of records updated
        """
        if not self.is_connected:
            self.connect()
            
        try:
            # Convert data types to Access-compatible formats
            for key, value in record.items():
                if isinstance(value, datetime):
                    record[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(value, bool):
                    record[key] = -1 if value else 0
                elif value is None:
                    record[key] = None
                    
            set_clause = ', '.join([f"{k} = ?" for k in record.keys()])
            query = f"UPDATE {table_name} SET {set_clause}"
            
            if condition:
                query += f" WHERE {condition}"
                
            self.cursor.execute(query, list(record.values()))
            affected_rows = self.cursor.rowcount
            
            if not self.in_transaction:
                self.conn.commit()
                
            return affected_rows
        except Exception as e:
            if self.in_transaction:
                self.rollback_transaction()
            raise DatabaseError(f"Failed to update record: {str(e)}")
    
    @handle_database_error
    def delete_records(self, table_name: str, condition: Optional[str] = None,
                      soft_delete: bool = False, cascade: bool = False) -> int:
        """
        Delete records from the specified table.
        
        Args:
            table_name: Name of the table to delete from
            condition: Optional WHERE clause condition
            soft_delete: If True, mark records as deleted instead of removing them
            cascade: If True, delete related records in other tables
            
        Returns:
            int: Number of records deleted
        """
        if not self.is_connected:
            self.connect()
            
        try:
            if soft_delete:
                # Update the is_deleted flag instead of deleting
                query = f"UPDATE {table_name} SET is_deleted = -1"
                if condition:
                    query += f" WHERE {condition}"
            else:
                if cascade:
                    # Get foreign key relationships
                    fk_info = self.get_foreign_keys(table_name)
                    for fk in fk_info:
                        # Delete related records first
                        self.delete_records(fk['referenced_table'], 
                                          f"{fk['referenced_column']} IN (SELECT {fk['column']} FROM {table_name} WHERE {condition})")
                
                query = f"DELETE FROM {table_name}"
                if condition:
                    query += f" WHERE {condition}"
                    
            self.cursor.execute(query)
            affected_rows = self.cursor.rowcount
            
            if not self.in_transaction:
                self.conn.commit()
                
            return affected_rows
        except Exception as e:
            if self.in_transaction:
                self.rollback_transaction()
            raise DatabaseError(f"Failed to delete records: {str(e)}")
    
    @handle_database_error
    def get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get foreign key relationships for a table.
        
        Args:
            table_name: Name of the table to get foreign keys for
            
        Returns:
            List of dictionaries containing foreign key information
        """
        if not self.is_connected:
            self.connect()
            
        try:
            query = """
                SELECT 
                    fk.name AS fk_name,
                    OBJECT_NAME(fk.parent_object_id) AS table_name,
                    c1.name AS column_name,
                    OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
                    c2.name AS referenced_column
                FROM 
                    sys.foreign_keys AS fk
                    INNER JOIN sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
                    INNER JOIN sys.columns AS c1 ON fkc.parent_object_id = c1.object_id AND fkc.parent_column_id = c1.column_id
                    INNER JOIN sys.columns AS c2 ON fkc.referenced_object_id = c2.object_id AND fkc.referenced_column_id = c2.column_id
                WHERE 
                    OBJECT_NAME(fk.parent_object_id) = ?
            """
            self.cursor.execute(query, [table_name])
            return [dict(zip([column[0] for column in self.cursor.description], row))
                   for row in self.cursor.fetchall()]
        except Exception as e:
            raise DatabaseError(f"Failed to get foreign keys: {str(e)}")

    @handle_database_error
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
            self.begin_transaction()  # Start transaction
            
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
                self.execute_many(query, values)
                total_inserted += len(batch)
                
                self.logger.info(f"Inserted batch of {len(batch)} records into temporary table")
            
            # Copy from temporary table to target table
            copy_query = f"""
                INSERT INTO [{table_name}]
                SELECT * FROM [{temp_table}]
            """
            self.execute_query(copy_query)
            self.commit_transaction()  # Commit transaction
            
            self.logger.info(f"Successfully inserted {total_inserted} records into {table_name}")
            return total_inserted
            
        except Exception as e:
            self.rollback_transaction()  # Rollback on error
            self.logger.error(f"Error during batch insert into {table_name}: {e}")
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
                self.execute_query(update_query, tuple(update_values))
                self.logger.info(f"Updated existing record in {table_name}")
            else:
                # Insert new record
                return self.insert_data(table_name, record)
                
            self.conn.commit()
            return 1
            
        except pyodbc.Error as e:
            self.logger.error(f"Error during upsert into {table_name}: {e}")
            self.conn.rollback()
            raise

    @handle_database_error
    def read_records(self, table_name: str, 
                    filters: Optional[Dict[str, Any]] = None,
                    sort_by: Optional[List[str]] = None,
                    sort_desc: bool = False,
                    limit: Optional[int] = None,
                    offset: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Read records from a table with filtering, sorting, and pagination.
        
        Args:
            table_name: Name of the table to read from
            filters: Dictionary of column names and values to filter by
            sort_by: List of column names to sort by
            sort_desc: Whether to sort in descending order
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of dictionaries containing the records
        """
        if not self.cursor:
            self.connect()
            
        # Start building the query
        query = f"SELECT * FROM [{table_name}]"
        params = []
        
        # Add WHERE clause if filters are provided
        if filters:
            conditions = []
            for column, value in filters.items():
                conditions.append(f"[{column}] = ?")
                params.append(value)
            query += " WHERE " + " AND ".join(conditions)
            
        # Add ORDER BY clause
        if sort_by:
            direction = "DESC" if sort_desc else "ASC"
            query += " ORDER BY " + ", ".join(f"[{col}] {direction}" for col in sort_by)
            
        # Handle pagination using Access-compatible syntax
        if limit is not None:
            if offset is not None and offset > 0:
                # For offset in Access, we need nested queries
                query = f"""
                    SELECT TOP {limit} *
                    FROM (
                        SELECT TOP {limit + offset} *
                        FROM ({query}) AS inner_query
                        {" ORDER BY " + ", ".join(f"[{col}] {'DESC' if sort_desc else 'ASC'}" for col in sort_by) if sort_by else ""}
                    ) AS offset_query
                    {" ORDER BY " + ", ".join(f"[{col}] {'ASC' if sort_desc else 'DESC'}" for col in sort_by) if sort_by else ""}
                """
            else:
                query = f"SELECT TOP {limit} * FROM ({query}) AS limit_query"
        
        return self.execute_query(query, tuple(params) if params else None)

    def build_query(self, table_name: str,
                   columns: Optional[List[str]] = None,
                   joins: Optional[List[Dict[str, str]]] = None,
                   filters: Optional[Dict[str, Any]] = None,
                   group_by: Optional[List[str]] = None,
                   having: Optional[Dict[str, Any]] = None,
                   sort_by: Optional[List[str]] = None,
                   sort_desc: bool = False,
                   limit: Optional[int] = None,
                   offset: Optional[int] = None) -> str:
        """
        Build a complex SQL query with multiple clauses.
        
        Args:
            table_name: Name of the main table
            columns: List of columns to select (None for all columns)
            joins: List of join dictionaries with 'table', 'on', and 'type' keys
            filters: Dictionary of column names and values to filter by
            group_by: List of columns to group by
            having: Dictionary of column names and values for HAVING clause
            sort_by: List of columns to sort by
            sort_desc: Whether to sort in descending order
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            SQL query string
        """
        # Build SELECT clause
        if columns:
            select_clause = ", ".join(f"[{col}]" for col in columns)
        else:
            select_clause = "*"
            
        query = f"SELECT {select_clause} FROM [{table_name}]"
        
        # Add JOIN clauses
        if joins:
            for join in joins:
                join_type = join.get('type', 'INNER').upper()
                query += f" {join_type} JOIN [{join['table']}] ON {join['on']}"
                
        # Add WHERE clause
        if filters:
            conditions = []
            for column, value in filters.items():
                conditions.append(f"[{column}] = ?")
            query += " WHERE " + " AND ".join(conditions)
            
        # Add GROUP BY clause
        if group_by:
            query += " GROUP BY " + ", ".join(f"[{col}]" for col in group_by)
            
        # Add HAVING clause
        if having:
            conditions = []
            for column, value in having.items():
                conditions.append(f"[{column}] = ?")
            query += " HAVING " + " AND ".join(conditions)
            
        # Add ORDER BY clause
        if sort_by:
            direction = "DESC" if sort_desc else "ASC"
            query += " ORDER BY " + ", ".join(f"[{col}] {direction}" for col in sort_by)
            
        # Add LIMIT and OFFSET
        if limit is not None:
            query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"
                
        return query 

    def aggregate_query(self, table_name: str,
                       aggregates: Dict[str, str],
                       group_by: Optional[List[str]] = None,
                       filters: Optional[Dict[str, Any]] = None,
                       having: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute an aggregate query with grouping and filtering.
        
        Args:
            table_name: Name of the table
            aggregates: Dictionary of column names and aggregate functions
                       (e.g., {'total': 'SUM(amount)', 'count': 'COUNT(*)'})
            group_by: List of columns to group by
            filters: Dictionary of column names and values to filter by
            having: Dictionary of aggregate conditions for HAVING clause
            
        Returns:
            List of dictionaries containing the aggregate results
        """
        if not self.cursor:
            self.connect()
            
        # Build SELECT clause with aggregates
        select_parts = []
        for alias, expr in aggregates.items():
            select_parts.append(f"{expr} AS [{alias}]")
        select_clause = ", ".join(select_parts)
        
        # Build the base query
        query = f"SELECT {select_clause} FROM [{table_name}]"
        
        # Add WHERE clause if filters are provided
        params = []
        if filters:
            conditions = []
            for column, value in filters.items():
                conditions.append(f"[{column}] = ?")
                params.append(value)
            query += " WHERE " + " AND ".join(conditions)
            
        # Add GROUP BY clause
        if group_by:
            query += " GROUP BY " + ", ".join(f"[{col}]" for col in group_by)
            
        # Add HAVING clause
        if having:
            conditions = []
            for column, value in having.items():
                conditions.append(f"[{column}] = ?")
                params.append(value)
            query += " HAVING " + " AND ".join(conditions)
            
        # Execute the query
        return self.execute_query(query, tuple(params) if params else None)
        
    def subquery(self, table_name: str,
                subquery: str,
                subquery_params: Optional[tuple] = None,
                filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a query with a subquery.
        
        Args:
            table_name: Name of the main table
            subquery: SQL subquery string
            subquery_params: Parameters for the subquery
            filters: Dictionary of column names and values to filter by
            
        Returns:
            List of dictionaries containing the results
        """
        if not self.cursor:
            self.connect()
            
        # Build the base query with subquery
        query = f"SELECT * FROM [{table_name}] WHERE EXISTS ({subquery})"
        
        # Add additional filters if provided
        params = list(subquery_params) if subquery_params else []
        if filters:
            conditions = []
            for column, value in filters.items():
                conditions.append(f"[{column}] = ?")
                params.append(value)
            query += " AND " + " AND ".join(conditions)
            
        # Execute the query
        return self.execute_query(query, tuple(params) if params else None)

    @handle_database_error
    def create_table(self, table_name: str, columns: Dict[str, str], 
                    primary_key: Optional[List[str]] = None) -> None:
        """
        Create a new table with the specified columns.
        
        Args:
            table_name: Name of the table to create
            columns: Dictionary of column names and their types
            primary_key: Optional list of primary key columns
        """
        if not self.cursor:
            self.connect()
            
        # Build column definitions
        column_defs = []
        for col_name, col_type in columns.items():
            # Convert common SQL types to Access types
            if col_type.upper() == 'TEXT':
                col_type = 'VARCHAR(255)'
            elif col_type.upper() == 'INTEGER':
                col_type = 'LONG'
            elif col_type.upper() == 'FLOAT':
                col_type = 'DOUBLE'
            elif col_type.upper() == 'DATETIME':
                col_type = 'DATE'
            elif col_type.upper() == 'BOOLEAN':
                col_type = 'BIT'
                
            column_defs.append(f"[{col_name}] {col_type}")
            
        # Add primary key constraint if specified
        if primary_key:
            pk_cols = ', '.join(f"[{col}]" for col in primary_key)
            column_defs.append(f"CONSTRAINT pk_{table_name} PRIMARY KEY ({pk_cols})")
            
        # Create table query
        query = f"""
            CREATE TABLE [{table_name}] (
                {', '.join(column_defs)}
            )
        """
        
        self.execute_query(query)
        if not self.in_transaction:
            self.conn.commit()

    @handle_database_error
    def alter_table(self, table_name: str, 
                   add_columns: Optional[Dict[str, str]] = None,
                   drop_columns: Optional[List[str]] = None) -> None:
        """
        Alter an existing table.
        
        Args:
            table_name: Name of the table to alter
            add_columns: Dictionary of columns to add and their types
            drop_columns: List of columns to drop
        """
        if not self.cursor:
            self.connect()
            
        # Add columns
        if add_columns:
            for col_name, col_type in add_columns.items():
                # Convert common SQL types to Access types
                if col_type.upper() == 'TEXT':
                    col_type = 'VARCHAR(255)'
                elif col_type.upper() == 'INTEGER':
                    col_type = 'LONG'
                elif col_type.upper() == 'FLOAT':
                    col_type = 'DOUBLE'
                elif col_type.upper() == 'DATETIME':
                    col_type = 'DATE'
                elif col_type.upper() == 'BOOLEAN':
                    col_type = 'BIT'
                    
                query = f"ALTER TABLE [{table_name}] ADD COLUMN [{col_name}] {col_type}"
                self.execute_query(query)
                
        # Drop columns
        if drop_columns:
            for col_name in drop_columns:
                query = f"ALTER TABLE [{table_name}] DROP COLUMN [{col_name}]"
                self.execute_query(query)
                
        if not self.in_transaction:
            self.conn.commit()
            
    @handle_database_error
    def add_foreign_key(self, table_name: str, column: str, 
                       ref_table: str, ref_column: str) -> None:
        """
        Add a foreign key constraint to a table.
        
        Args:
            table_name: Name of the table
            column: Name of the foreign key column
            ref_table: Name of the referenced table
            ref_column: Name of the referenced column
        """
        if not self.cursor:
            self.connect()
            
        constraint_name = f"fk_{table_name}_{column}"
        query = f"""
            ALTER TABLE [{table_name}]
            ADD CONSTRAINT [{constraint_name}]
            FOREIGN KEY ([{column}])
            REFERENCES [{ref_table}] ([{ref_column}])
        """
        
        self.execute_query(query)
        if not self.in_transaction:
            self.conn.commit()

    @handle_database_error
    def update_data(self, table: str, data: Dict[str, Any], 
                   where: str, where_params: Dict[str, Any]) -> int:
        """
        Update data in a table.
        
        Args:
            table: Table name
            data: Dictionary of column names and values to update
            where: WHERE clause
            where_params: Parameters for the WHERE clause
            
        Returns:
            Number of affected rows
        """
        set_clause = ', '.join(f"[{k}] = ?" for k in data.keys())
        query = f"UPDATE [{table}] SET {set_clause} WHERE {where}"
        
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, list(data.values()) + list(where_params.values()))
            return cursor.rowcount
        finally:
            cursor.close()

    @handle_database_error
    def delete_data(self, table: str, where: str, 
                   where_params: Dict[str, Any]) -> int:
        """
        Delete data from a table.
        
        Args:
            table: Table name
            where: WHERE clause
            where_params: Parameters for the WHERE clause
            
        Returns:
            Number of affected rows
        """
        query = f"DELETE FROM [{table}] WHERE {where}"
        
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, where_params)
            return cursor.rowcount
        finally:
            cursor.close()

    @handle_database_error
    def execute_many(self, query: str, values: List[List[Any]]) -> int:
        """
        Execute a query with multiple sets of parameters.

        Args:
            query: SQL query to execute
            values: List of parameter lists

        Returns:
            Total number of rows affected
        """
        if not self.cursor:
            self.connect()

        total_affected = 0
        for value_set in values:
            self.cursor.execute(query, value_set)
            total_affected += self.cursor.rowcount
        return total_affected 