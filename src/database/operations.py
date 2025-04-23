import pyodbc
import logging

logger = logging.getLogger(__name__)

class DatabaseOperations:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None
        self.transaction = None
        
    def connect(self):
        """Establish database connection."""
        try:
            self.connection = pyodbc.connect(self.connection_string)
            return True
        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            return False
            
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            
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