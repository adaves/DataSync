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

class DatabaseError(Exception):
    """Custom exception for database operations."""
    pass

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

    def read_records(self, table_name: str, 
                    filters: Optional[Dict[str, Any]] = None,
                    sort_by: Optional[List[str]] = None,
                    sort_desc: bool = False,
                    limit: Optional[int] = None,
                    offset: Optional[int] = None,
                    date_range: Optional[Dict[str, Any]] = None,
                    custom_filters: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Read records from a table with filtering, sorting, and pagination.
        
        Args:
            table_name: Name of the table to read from
            filters: Dictionary of column names and values to filter by
            sort_by: List of column names to sort by
            sort_desc: Whether to sort in descending order
            limit: Maximum number of records to return
            offset: Number of records to skip
            date_range: Dictionary with 'column', 'start_date', and 'end_date' for date range filtering
            custom_filters: List of custom SQL filter expressions
            
        Returns:
            List of dictionaries containing the records
        """
        if not self.cursor:
            self.connect()
            
        # Build the base query
        query = f"SELECT * FROM [{table_name}]"
        
        # Add WHERE clause if filters are provided
        params = []
        conditions = []
        
        # Add standard filters
        if filters:
            for column, value in filters.items():
                if isinstance(value, (list, tuple)):
                    # Handle IN clause
                    placeholders = ", ".join(["?" for _ in value])
                    conditions.append(f"[{column}] IN ({placeholders})")
                    params.extend(value)
                else:
                    conditions.append(f"[{column}] = ?")
                    params.append(value)
        
        # Add date range filter
        if date_range:
            column = date_range.get('column')
            start_date = date_range.get('start_date')
            end_date = date_range.get('end_date')
            
            if column and (start_date or end_date):
                if start_date:
                    conditions.append(f"[{column}] >= ?")
                    params.append(start_date)
                if end_date:
                    conditions.append(f"[{column}] <= ?")
                    params.append(end_date)
        
        # Add custom filters
        if custom_filters:
            conditions.extend(custom_filters)
        
        # Combine all conditions
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        # Add ORDER BY clause if sort_by is provided
        if sort_by:
            direction = "DESC" if sort_desc else "ASC"
            query += " ORDER BY " + ", ".join(f"[{col}] {direction}" for col in sort_by)
            
        # Add LIMIT and OFFSET if provided
        if limit is not None:
            query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"
                
        # Execute the query
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

    def update_record(self, table_name: str, record: Dict[str, Any], 
                     key_columns: List[str]) -> int:
        """
        Update a single record in a table.
        
        Args:
            table_name: Name of the table to update
            record: Dictionary containing column names and values
            key_columns: List of column names that form the unique key
            
        Returns:
            Number of records updated (1 if successful)
        """
        if not self.cursor:
            self.connect()
            
        try:
            # Build WHERE clause for key columns
            where_clause = ' AND '.join([f'[{col}] = ?' for col in key_columns])
            key_values = [record[col] for col in key_columns]
            
            # Build SET clause for non-key columns
            update_cols = [col for col in record.keys() if col not in key_columns]
            set_clause = ', '.join([f'[{col}] = ?' for col in update_cols])
            update_values = [record[col] for col in update_cols]
            
            # Combine all values (update values first, then key values)
            all_values = update_values + key_values
            
            # Build and execute the query
            query = f"""
                UPDATE [{table_name}]
                SET {set_clause}
                WHERE {where_clause}
            """
            
            self.cursor.execute(query, all_values)
            self.conn.commit()
            
            self.logger.info(f"Successfully updated record in {table_name}")
            return 1
            
        except pyodbc.Error as e:
            self.logger.error(f"Error updating record in {table_name}: {e}")
            self.conn.rollback()
            raise

    def batch_update(self, table_name: str, records: List[Dict[str, Any]], 
                    key_columns: List[str], batch_size: int = 1000) -> int:
        """
        Update multiple records in a table in batches.
        
        Args:
            table_name: Name of the table to update
            records: List of dictionaries containing column names and values
            key_columns: List of column names that form the unique key
            batch_size: Number of records to update in each batch
            
        Returns:
            Total number of records updated
        """
        if not records:
            return 0
            
        if not self.cursor:
            self.connect()
            
        total_updated = 0
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
                    
                # Prepare batch update query
                update_cols = [col for col in columns if col not in key_columns]
                set_clause = ', '.join([f't.[{col}] = s.[{col}]' for col in update_cols])
                where_clause = ' AND '.join([f't.[{col}] = s.[{col}]' for col in key_columns])
                
                query = f"""
                    UPDATE t
                    SET {set_clause}
                    FROM [{table_name}] t
                    INNER JOIN [{temp_table}] s
                    ON {where_clause}
                """
                
                # Insert batch into temporary table
                self.batch_insert(temp_table, batch, batch_size)
                
                # Execute the update
                self.cursor.execute(query)
                total_updated += len(batch)
                
                self.logger.info(f"Updated batch of {len(batch)} records")
            
            self.conn.commit()
            self.logger.info(f"Successfully updated {total_updated} records in {table_name}")
            return total_updated
            
        except pyodbc.Error as e:
            self.logger.error(f"Error during batch update in {table_name}: {e}")
            self.conn.rollback()
            raise
            
        finally:
            # Clean up temporary table
            if temp_table:
                try:
                    self.execute_query(f"DROP TABLE [{temp_table}]")
                except Exception as e:
                    self.logger.warning(f"Failed to clean up temporary table: {e}")

    def update_with_conditions(self, table_name: str, 
                             updates: Dict[str, Any],
                             conditions: Dict[str, Any]) -> int:
        """
        Update records in a table based on conditions.
        
        Args:
            table_name: Name of the table to update
            updates: Dictionary of column names and new values
            conditions: Dictionary of column names and values to filter by
            
        Returns:
            Number of records updated
        """
        if not self.cursor:
            self.connect()
            
        try:
            # Build SET clause
            set_clause = ', '.join([f'[{col}] = ?' for col in updates.keys()])
            update_values = list(updates.values())
            
            # Build WHERE clause
            where_clause = ' AND '.join([f'[{col}] = ?' for col in conditions.keys()])
            condition_values = list(conditions.values())
            
            # Combine all values
            all_values = update_values + condition_values
            
            # Build and execute the query
            query = f"""
                UPDATE [{table_name}]
                SET {set_clause}
                WHERE {where_clause}
            """
            
            self.cursor.execute(query, all_values)
            self.conn.commit()
            
            affected_rows = self.cursor.rowcount
            self.logger.info(f"Updated {affected_rows} records in {table_name}")
            return affected_rows
            
        except pyodbc.Error as e:
            self.logger.error(f"Error updating records in {table_name}: {e}")
            self.conn.rollback()
            raise 

    def delete_records(self, table_name: str, conditions: Dict[str, Any] = None) -> int:
        """
        Delete records from a table based on conditions.
        
        Args:
            table_name (str): Name of the table to delete from
            conditions (Dict[str, Any], optional): Conditions for deletion
            
        Returns:
            int: Number of records deleted
            
        Example:
            >>> db.delete_records("products", {"category": "discontinued"})
            5  # 5 records deleted
        """
        try:
            if not self.conn:
                self.connect()
            
            # Build the DELETE query
            query = f"DELETE FROM [{table_name}]"
            params = []
            
            if conditions:
                where_clause = " AND ".join([f"[{k}] = ?" for k in conditions.keys()])
                query += f" WHERE {where_clause}"
                params.extend(conditions.values())
            
            # Execute the query
            self.cursor.execute(query, params)
            self.conn.commit()
            
            return self.cursor.rowcount
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            raise DatabaseError(f"Error deleting records: {str(e)}")
    
    def soft_delete(self, table_name: str, record_id: Any, id_column: str = "id") -> bool:
        """
        Perform a soft delete by setting a deleted flag instead of removing the record.
        
        Args:
            table_name (str): Name of the table
            record_id (Any): ID of the record to soft delete
            id_column (str, optional): Name of the ID column. Defaults to "id".
            
        Returns:
            bool: True if the record was soft deleted, False otherwise
            
        Example:
            >>> db.soft_delete("products", 123)
            True
        """
        try:
            if not self.conn:
                self.connect()
            
            # Check if the table has a deleted_at column
            columns = self.get_table_columns(table_name)
            if "deleted_at" not in columns:
                raise DatabaseError(f"Table {table_name} does not support soft delete")
            
            # Update the deleted_at timestamp
            query = f"""
                UPDATE [{table_name}]
                SET deleted_at = ?
                WHERE [{id_column}] = ? AND deleted_at IS NULL
            """
            self.cursor.execute(query, (datetime.now(), record_id))
            self.conn.commit()
            
            return self.cursor.rowcount > 0
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            raise DatabaseError(f"Error performing soft delete: {str(e)}")
    
    def cascade_delete(self, table_name: str, record_id: Any, 
                      cascade_tables: List[Dict[str, Any]], 
                      id_column: str = "id") -> Dict[str, int]:
        """
        Delete a record and all related records in cascade tables.
        
        Args:
            table_name (str): Name of the main table
            record_id (Any): ID of the record to delete
            cascade_tables (List[Dict[str, Any]]): List of cascade table configurations
                Each dict should contain:
                - table: Name of the cascade table
                - foreign_key: Name of the foreign key column
                - cascade_type: "delete" or "nullify"
            id_column (str, optional): Name of the ID column. Defaults to "id".
            
        Returns:
            Dict[str, int]: Number of records deleted in each table
            
        Example:
            >>> cascade_config = [
            ...     {"table": "order_items", "foreign_key": "product_id", "cascade_type": "delete"},
            ...     {"table": "product_reviews", "foreign_key": "product_id", "cascade_type": "nullify"}
            ... ]
            >>> db.cascade_delete("products", 123, cascade_config)
            {"products": 1, "order_items": 5, "product_reviews": 3}
        """
        try:
            if not self.conn:
                self.connect()
            
            self.begin_transaction()
            results = {}
            
            try:
                # Process each cascade table
                for cascade in cascade_tables:
                    cascade_table = cascade["table"]
                    foreign_key = cascade["foreign_key"]
                    cascade_type = cascade["cascade_type"]
                    
                    if cascade_type == "delete":
                        # Delete related records
                        query = f"""
                            DELETE FROM [{cascade_table}]
                            WHERE [{foreign_key}] = ?
                        """
                        self.cursor.execute(query, (record_id,))
                        results[cascade_table] = self.cursor.rowcount
                    elif cascade_type == "nullify":
                        # Set foreign key to NULL
                        query = f"""
                            UPDATE [{cascade_table}]
                            SET [{foreign_key}] = NULL
                            WHERE [{foreign_key}] = ?
                        """
                        self.cursor.execute(query, (record_id,))
                        results[cascade_table] = self.cursor.rowcount
                
                # Delete the main record
                query = f"""
                    DELETE FROM [{table_name}]
                    WHERE [{id_column}] = ?
                """
                self.cursor.execute(query, (record_id,))
                results[table_name] = self.cursor.rowcount
                
                self.commit_transaction()
                return results
                
            except Exception as e:
                self.rollback_transaction()
                raise DatabaseError(f"Error during cascade delete: {str(e)}")
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            raise DatabaseError(f"Error performing cascade delete: {str(e)}")
    
    def delete_with_transaction(self, table_name: str, conditions: Dict[str, Any] = None) -> int:
        """
        Delete records within a transaction, with rollback on error.
        
        Args:
            table_name (str): Name of the table to delete from
            conditions (Dict[str, Any], optional): Conditions for deletion
            
        Returns:
            int: Number of records deleted
            
        Example:
            >>> db.delete_with_transaction("orders", {"status": "cancelled"})
            3  # 3 records deleted
        """
        try:
            if not self.conn:
                self.connect()
            
            self.begin_transaction()
            
            try:
                # Build the DELETE query
                query = f"DELETE FROM [{table_name}]"
                params = []
                
                if conditions:
                    where_clause = " AND ".join([f"[{k}] = ?" for k in conditions.keys()])
                    query += f" WHERE {where_clause}"
                    params.extend(conditions.values())
                
                # Execute the query
                self.cursor.execute(query, params)
                affected_rows = self.cursor.rowcount
                
                self.commit_transaction()
                return affected_rows
                
            except Exception as e:
                self.rollback_transaction()
                raise DatabaseError(f"Error during delete transaction: {str(e)}")
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            raise DatabaseError(f"Error performing delete with transaction: {str(e)}") 