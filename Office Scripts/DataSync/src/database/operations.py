import pyodbc
import logging

logger = logging.getLogger(__name__)

class DatabaseOperations:
    def __init__(self, db_path: str):
        """Initialize with database path and create connection string."""
        import os
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        # Create proper Access connection string with fallback drivers
        self.db_path = os.path.abspath(db_path)
        self.connection_string = self._get_access_connection_string()
        self.connection = None
        self.transaction = None
        
    def _get_access_connection_string(self):
        """Get the appropriate Access ODBC connection string with driver detection."""
        # Try different Access driver names in order of preference
        drivers = [
            "Microsoft Access Driver (*.mdb, *.accdb)",  # Most common
            "Microsoft Access Driver (*.mdb)",           # Older systems
            "Driver do Microsoft Access (*.mdb, *.accdb)", # Portuguese
            "Microsoft Access-Treiber (*.mdb, *.accdb)"    # German
        ]
        
        # Check available drivers
        try:
            available_drivers = [x for x in pyodbc.drivers() if 'access' in x.lower()]
            logger.info(f"Available Access drivers: {available_drivers}")
            
            # Use the first available driver from our preferred list
            for driver in drivers:
                if driver in available_drivers:
                    return f"DRIVER={{{driver}}};DBQ={self.db_path};"
                    
            # If none of our preferred drivers are found, use the first available Access driver
            if available_drivers:
                return f"DRIVER={{{available_drivers[0]}}};DBQ={self.db_path};"
                
        except Exception as e:
            logger.warning(f"Could not enumerate ODBC drivers: {e}")
            
        # Fallback to the most common driver name
        return f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={self.db_path};"
        
    def connect(self):
        """Establish database connection."""
        try:
            logger.info(f"Attempting to connect to database: {self.db_path}")
            logger.info(f"Using connection string: {self.connection_string}")
            self.connection = pyodbc.connect(self.connection_string)
            logger.info("Database connection successful")
            return True
        except pyodbc.Error as e:
            error_msg = str(e)
            if "IM002" in error_msg:
                # ODBC Driver Manager error - driver not found
                available_drivers = []
                try:
                    available_drivers = [x for x in pyodbc.drivers() if 'access' in x.lower()]
                except:
                    pass
                    
                logger.error("ODBC Access driver not found or not properly configured")
                logger.error(f"Available Access drivers: {available_drivers}")
                logger.error("Please install Microsoft Access Database Engine or Office with Access")
                logger.error("Download from: https://www.microsoft.com/en-us/download/details.aspx?id=54920")
            else:
                logger.error(f"Database connection error: {error_msg}")
            return False
        except Exception as e:
            logger.error(f"Unexpected connection error: {str(e)}")
            return False
            
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def close(self):
        """Alias for disconnect() - used by CLI."""
        self.disconnect()
    
    def get_tables(self):
        """Get list of table names in the database."""
        try:
            if not self.connection:
                self.connect()
            cursor = self.connection.cursor()
            
            # Get all user tables (exclude system tables)
            tables = []
            for table_info in cursor.tables(tableType='TABLE'):
                table_name = table_info.table_name
                if not table_name.startswith('MSys') and not table_name.startswith('~'):
                    tables.append(table_name)
            
            cursor.close()
            return tables
        except Exception as e:
            logger.error(f"Error getting tables: {str(e)}")
            raise
    
    def read_table(self, table_name: str):
        """Read all records from a table and return as pandas DataFrame."""
        try:
            import pandas as pd
            if not self.connection:
                self.connect()
            
            query = f"SELECT * FROM [{table_name}]"
            df = pd.read_sql(query, self.connection)
            return df
        except Exception as e:
            logger.error(f"Error reading table {table_name}: {str(e)}")
            raise
    
    def add_record(self, table: str, record: dict):
        """Add a single record to the table."""
        return self.insert_record(table, record)
    
    def insert_records_batch(self, table: str, records: list, batch_size: int = 1000):
        """Insert multiple records in batches for better performance."""
        try:
            if not self.connection:
                self.connect()
            
            total_inserted = 0
            
            # Process records in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                cursor = self.connection.cursor()
                
                try:
                    for record in batch:
                        if not record:  # Skip empty records
                            continue
                            
                        # Build the insert query
                        columns = ', '.join([f'[{k}]' for k in record.keys()])
                        placeholders = ', '.join(['?' for _ in record])
                        query = f"INSERT INTO [{table}] ({columns}) VALUES ({placeholders})"
                        
                        # Execute the query
                        cursor.execute(query, tuple(record.values()))
                        total_inserted += 1
                    
                    # Commit the batch
                    self.connection.commit()
                    logger.info(f"Committed batch of {len(batch)} records (total: {total_inserted})")
                    
                except Exception as e:
                    # Rollback the batch on error
                    self.connection.rollback()
                    logger.error(f"Error in batch insert, rolled back: {e}")
                    raise
                finally:
                    cursor.close()
            
            return total_inserted
            
        except Exception as e:
            logger.error(f"Batch insert error: {str(e)}")
            raise
    
    def convert_data_for_access(self, data: dict, table_schema: list) -> dict:
        """Convert data types to be compatible with Access database."""
        import pandas as pd
        from datetime import datetime
        
        converted_data = {}
        
        # Create a mapping of column names to their types
        schema_map = {col['column_name']: col['data_type'] for col in table_schema}
        
        for key, value in data.items():
            if key not in schema_map:
                # Skip columns not in the database table
                continue
                
            db_type = schema_map[key].upper()
            
            # Handle None/NaN values
            if pd.isna(value) or value is None:
                converted_data[key] = None
                continue
            
            try:
                # Convert based on Access data types
                if 'VARCHAR' in db_type or 'TEXT' in db_type or 'CHAR' in db_type:
                    # Text fields
                    converted_data[key] = str(value).strip()
                elif 'INTEGER' in db_type or 'LONG' in db_type or 'COUNTER' in db_type:
                    # Integer fields
                    if isinstance(value, str) and value.strip() == '':
                        converted_data[key] = None
                    else:
                        # Handle comma-separated numbers in integer fields
                        if isinstance(value, str):
                            clean_value = value.replace(',', '').strip()
                            converted_data[key] = int(float(clean_value))  # Handle decimal strings
                        else:
                            converted_data[key] = int(float(value))
                elif 'SINGLE' in db_type or 'DOUBLE' in db_type or 'DECIMAL' in db_type or 'NUMBER' in db_type:
                    # Numeric fields
                    if isinstance(value, str) and value.strip() == '':
                        converted_data[key] = None
                    else:
                        # Handle percentage strings - preserve the percentage format
                        if isinstance(value, str) and '%' in value:
                            # For percentage fields, extract the numeric value but keep it as the displayed percentage
                            # E.g., "15.47%" -> 15.47 (not 0.1547)
                            numeric_str = value.replace('%', '').replace(',', '').strip()
                            if numeric_str:
                                converted_data[key] = float(numeric_str)
                            else:
                                converted_data[key] = None
                        else:
                            # Handle comma-separated numbers (e.g., "2,701" -> 2701.0)
                            if isinstance(value, str):
                                # Remove commas from number strings
                                clean_value = value.replace(',', '').strip()
                                converted_data[key] = float(clean_value)
                            else:
                                converted_data[key] = float(value)
                elif 'DATETIME' in db_type or 'DATE' in db_type:
                    # Date/DateTime fields
                    if isinstance(value, str):
                        if value.strip() == '':
                            converted_data[key] = None
                        else:
                            # Try to parse date string
                            converted_data[key] = pd.to_datetime(value).to_pydatetime()
                    elif isinstance(value, (datetime, pd.Timestamp)):
                        converted_data[key] = value
                    else:
                        converted_data[key] = None
                elif 'BIT' in db_type or 'LOGICAL' in db_type:
                    # Boolean fields
                    if isinstance(value, str):
                        converted_data[key] = value.lower() in ('true', 'yes', '1', 'on')
                    else:
                        converted_data[key] = bool(value)
                else:
                    # Default: convert to string
                    converted_data[key] = str(value)
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not convert value '{value}' for column '{key}' (type: {db_type}): {e}")
                converted_data[key] = None
        
        return converted_data
            
    def begin_transaction(self):
        """Start a new transaction."""
        if not self.connection:
            self.connect()
        self.transaction = self.connection.cursor()
        return self.transaction
        
    def commit(self):
        """Commit the current transaction."""
        if self.transaction:
            self.connection.commit()
            self.transaction = None
            
    def rollback(self):
        """Roll back the current transaction."""
        if self.transaction:
            self.connection.rollback()
            self.transaction = None
            
    @property
    def in_transaction(self):
        """Check if there is an active transaction."""
        return self.transaction is not None
            
    def execute_query(self, query: str, params: tuple = None) -> list:
        """Execute a SQL query and return results."""
        try:
            if not self.connection:
                self.connect()
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            try:
                results = cursor.fetchall()
            except pyodbc.ProgrammingError:
                results = []
            cursor.close()
            return results
        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            raise
            
    def insert_record(self, table: str, data: dict) -> int:
        """Insert a single record into the specified table."""
        try:
            if not self.connection:
                self.connect()
            cursor = self.connection.cursor()
            
            # Build the insert query
            columns = ', '.join([f'[{k}]' for k in data.keys()])
            placeholders = ', '.join(['?' for _ in data])
            query = f"INSERT INTO [{table}] ({columns}) VALUES ({placeholders})"
            
            # Execute the query
            cursor.execute(query, tuple(data.values()))
            
            # Commit the transaction to save the record
            self.connection.commit()
            
            # Get the last inserted ID
            cursor.execute("SELECT @@IDENTITY")
            last_id = cursor.fetchone()[0]
            cursor.close()
            
            return last_id
        except Exception as e:
            logger.error(f"Insert error: {str(e)}")
            raise
            
    def create_table(self, table_name: str, columns: dict, primary_key: list = None):
        """Create a new table with the specified columns."""
        try:
            if not self.connection:
                self.connect()
            cursor = self.connection.cursor()
            
            # Build the column definitions
            column_defs = []
            for col_name, col_type in columns.items():
                column_defs.append(f"[{col_name}] {col_type}")
                
            # Add primary key if specified
            if primary_key:
                pk_columns = ', '.join([f'[{col}]' for col in primary_key])
                column_defs.append(f"PRIMARY KEY ({pk_columns})")
                
            # Create the table
            query = f"CREATE TABLE [{table_name}] ({', '.join(column_defs)})"
            cursor.execute(query)
            self.connection.commit()
            cursor.close()
            
        except Exception as e:
            logger.error(f"Table creation error: {str(e)}")
            raise
            
    def add_foreign_key(self, table: str, column: str, ref_table: str, ref_column: str):
        """Add a foreign key constraint to a table."""
        try:
            if not self.connection:
                self.connect()
            cursor = self.connection.cursor()
            
            # Build the foreign key constraint name
            constraint_name = f"FK_{table}_{column}"
            
            # Add the foreign key
            query = f"""
            ALTER TABLE [{table}]
            ADD CONSTRAINT [{constraint_name}]
            FOREIGN KEY ([{column}])
            REFERENCES [{ref_table}] ([{ref_column}])
            """
            cursor.execute(query)
            self.connection.commit()
            cursor.close()
            
        except Exception as e:
            logger.error(f"Foreign key creation error: {str(e)}")
            raise
    
    def delete_records(self, table: str, where_clause: str, deletion_name: str = None):
        """Delete records from table based on where clause."""
        try:
            if not self.connection:
                self.connect()
            cursor = self.connection.cursor()
            
            query = f"DELETE FROM [{table}] WHERE {where_clause}"
            cursor.execute(query)
            rows_affected = cursor.rowcount
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Deleted {rows_affected} records from {table}")
            return rows_affected
        except Exception as e:
            logger.error(f"Delete error: {str(e)}")
            raise
    
    def list_deleted_records(self, source_table: str = None):
        """List deleted records (placeholder - returns empty list)."""
        # This would typically query a deleted_records audit table
        return []
    
    def recover_records(self, criteria: dict):
        """Recover deleted records (placeholder - returns 0)."""
        # This would typically restore from deleted_records audit table
        return 0
    
    def get_table_schema(self, table_name: str):
        """Get the schema/structure of a table including column names and types."""
        try:
            if not self.connection:
                self.connect()
            cursor = self.connection.cursor()
            
            # Get column information from the table
            columns_info = cursor.columns(table=table_name)
            schema = []
            for column in columns_info:
                schema.append({
                    'column_name': column.column_name,
                    'data_type': column.type_name,
                    'size': getattr(column, 'column_size', None),
                    'nullable': getattr(column, 'nullable', None)
                })
            cursor.close()
            return schema
        except Exception as e:
            logger.error(f"Error getting table schema: {str(e)}")
            raise
    
    @staticmethod
    def check_odbc_drivers():
        """Check available ODBC drivers and return diagnostic information."""
        try:
            all_drivers = pyodbc.drivers()
            access_drivers = [x for x in all_drivers if 'access' in x.lower()]
            
            return {
                'all_drivers_count': len(all_drivers),
                'access_drivers': access_drivers,
                'has_access_driver': len(access_drivers) > 0,
                'recommended_driver': 'Microsoft Access Driver (*.mdb, *.accdb)'
            }
        except Exception as e:
            return {
                'error': str(e),
                'has_access_driver': False,
                'access_drivers': []
            } 