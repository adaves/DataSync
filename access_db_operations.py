import pyodbc
import pandas as pd
from pathlib import Path
import os
import logging
from datetime import datetime
import shutil
from tqdm import tqdm
import time

class AccessDB:
    def __init__(self, db_path):
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
        
        # Setup logging
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"db_operations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self):
        """Establish connection to the database"""
        try:
            self.conn = pyodbc.connect(self.conn_str)
            self.cursor = self.conn.cursor()
            self.logger.info("Successfully connected to the database")
        except pyodbc.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            self.logger.error(f"Connection string used: {self.conn_str}")  # Debug info
            raise
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()
            self.logger.info("Database connection closed")
    
    def get_tables(self):
        """Get list of all tables in the database"""
        if not hasattr(self, 'cursor'):
            self.connect()
        
        tables = []
        for row in self.cursor.tables():
            if row.table_type == 'TABLE':
                tables.append(row.table_name)
        return tables
    
    def execute_query(self, query, params=None):
        """Execute a SQL query and return results"""
        if not hasattr(self, 'cursor'):
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
    
    def count_records(self, table_name, year, date_column='Time'):
        """Count records for a specific year in a table"""
        try:
            query = f"SELECT COUNT(*) as record_count FROM [{table_name}] WHERE YEAR([{date_column}]) = ?"
            result = self.execute_query(query, [year])
            return result[0]['record_count'] if result else 0
        except Exception as e:
            self.logger.error(f"Error counting records: {e}")
            raise
    
    def read_table(self, table_name):
        """Read an entire table into a pandas DataFrame"""
        if not hasattr(self, 'cursor'):
            self.connect()
        
        try:
            query = f"SELECT * FROM [{table_name}]"
            return pd.read_sql(query, self.conn)
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            raise
    
    def delete_year_data(self, table_name, year, date_column='Time', batch_size=1000):
        """Delete all records for a specific year from a table in batches"""
        try:
            # First count the records to be deleted
            record_count = self.count_records(table_name, year, date_column)
            
            if record_count == 0:
                self.logger.info(f"No records found for year {year}")
                return 0
            
            self.logger.info(f"Starting batch deletion of {record_count:,} records")
            
            # Since data is quarterly, we'll process each quarter separately
            total_deleted = 0
            
            # Define the quarterly dates
            quarterly_dates = [
                f"01/01/{year}",  # Q1
                f"04/01/{year}",  # Q2
                f"07/01/{year}",  # Q3
                f"10/01/{year}"   # Q4
            ]
            
            # Process one quarter at a time to reduce lock count
            for quarter, date in enumerate(quarterly_dates, 1):
                self.logger.info(f"Processing Q{quarter} ({date})")
                
                # Delete records for this quarter
                try:
                    query = f"""
                        DELETE FROM [{table_name}]
                        WHERE [{date_column}] = #{date}#
                    """
                    
                    self.cursor.execute(query)
                    count = self.cursor.rowcount
                    self.conn.commit()
                    
                    total_deleted += count
                    self.logger.info(f"Q{quarter}: Deleted {count} records")
                    
                except Exception as e:
                    self.logger.error(f"Error deleting data for Q{quarter}: {e}")
                    self.conn.rollback()
                    # Continue with next quarter
            
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
    
    def cleanup_temp_tables(self):
        """Clean up any leftover temporary tables"""
        try:
            # Get all tables
            tables = self.get_tables()
            # Find and drop any temp_batch_* tables
            for table in tables:
                if table.startswith('temp_batch_'):
                    try:
                        self.execute_query(f"DROP TABLE [{table}]")
                        self.logger.info(f"Cleaned up temporary table: {table}")
                    except Exception as e:
                        self.logger.warning(f"Failed to clean up temporary table {table}: {e}")
        except Exception as e:
            self.logger.warning(f"Error during temporary table cleanup: {e}")
    
    def process_excel_files_for_year(self, excel_dir, table_name, year):
        """Process Excel files for a specific year and insert data into the database"""
        excel_dir = Path(excel_dir)
        
        # Get all Excel files
        excel_files = list(excel_dir.glob('*.xlsx'))
        
        if not excel_files:
            self.logger.info("No Excel files found to process")
            return
        
        self.logger.info(f"Found {len(excel_files)} Excel files to process")
        
        for excel_file in excel_files:
            try:
                self.logger.info(f"Processing file: {excel_file.name}")
                
                # Read Excel file
                df = pd.read_excel(excel_file)
                
                # Convert 'Time' column to datetime if it's not already
                df['Time'] = pd.to_datetime(df['Time'])
                
                # Filter data for the specified year
                df = df[df['Time'].dt.year == year]
                
                if len(df) == 0:
                    self.logger.info(f"No data found for year {year} in {excel_file.name}")
                    continue
                
                # Insert data into database
                self.insert_dataframe(table_name, df)
                self.logger.info(f"Successfully inserted {len(df)} records from {excel_file.name}")
                
            except Exception as e:
                self.logger.error(f"Error processing file {excel_file.name}: {e}")
                continue

    def process_excel_files(self, excel_dir, processed_dir, table_name, year=None):
        """Process Excel files from a directory and move them to processed directory"""
        excel_dir = Path(excel_dir)
        
        # If year is specified, use the new method
        if year is not None:
            self.process_excel_files_for_year(excel_dir, table_name, year)
            return
            
        # Only create processed directory if we're moving files
        if processed_dir is not None:
            processed_dir = Path(processed_dir)
            processed_dir.mkdir(exist_ok=True)
        
        # Get all Excel files
        excel_files = list(excel_dir.glob('*.xlsx'))
        
        if not excel_files:
            self.logger.info("No Excel files found to process")
            return
        
        self.logger.info(f"Found {len(excel_files)} Excel files to process")
        
        for excel_file in excel_files:
            try:
                self.logger.info(f"Processing file: {excel_file.name}")
                
                # Read Excel file
                df = pd.read_excel(excel_file)
                
                # Insert data into database
                self.insert_dataframe(table_name, df)
                
                # Move file to processed directory only if processed_dir is provided
                if processed_dir is not None:
                    processed_file = processed_dir / excel_file.name
                    shutil.move(str(excel_file), str(processed_file))
                    self.logger.info(f"Moved {excel_file.name} to processed directory")
                
            except Exception as e:
                self.logger.error(f"Error processing file {excel_file.name}: {e}")
                continue
    
    def get_table_columns(self, table_name):
        """Get the column names and types from a table"""
        try:
            query = f"SELECT * FROM [{table_name}] WHERE 1=0"  # Get structure without data
            self.cursor.execute(query)
            columns = [column[0] for column in self.cursor.description]
            return columns
        except Exception as e:
            self.logger.error(f"Error getting table columns: {e}")
            raise

    def insert_dataframe(self, table_name, df):
        """Insert a pandas DataFrame into the database table"""
        try:
            # Get the actual table columns
            table_columns = self.get_table_columns(table_name)
            self.logger.info(f"Table columns: {table_columns}")
            
            # Get Excel columns
            excel_columns = df.columns.tolist()
            self.logger.info(f"Excel columns: {excel_columns}")
            
            # Create a mapping of Excel columns to table columns
            column_mapping = {}
            
            # Define specific column name mappings
            special_mappings = {
                'Quota (Standard Cases)': 'Quota Std Cases',
                'Quota (Net Sales)': 'Quota Net Sales',
                'Quota (GM $)': 'Quota GM $'
            }
            
            for excel_col in excel_columns:
                # Check for special mapping first
                if excel_col in special_mappings:
                    column_mapping[excel_col] = special_mappings[excel_col]
                # Try exact match
                elif excel_col in table_columns:
                    column_mapping[excel_col] = excel_col
                else:
                    # Try case-insensitive match
                    for table_col in table_columns:
                        if excel_col.lower() == table_col.lower():
                            column_mapping[excel_col] = table_col
                            break
            
            # Check if all Excel columns have a match
            missing_columns = [col for col in excel_columns if col not in column_mapping]
            if missing_columns:
                self.logger.warning(f"Some columns from Excel don't match table columns: {missing_columns}")
            
            # Prepare the insert query with only matching columns
            matching_columns = [column_mapping[col] for col in excel_columns if col in column_mapping]
            placeholders = ','.join(['?' for _ in matching_columns])
            
            # Properly escape column names with special characters
            escaped_columns = [f'[{col}]' for col in matching_columns]
            query = f"INSERT INTO [{table_name}] ({','.join(escaped_columns)}) VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples for insertion
            data = []
            for _, row in df.iterrows():
                row_data = []
                for col in excel_columns:
                    if col in column_mapping:
                        value = row[col]
                        # Convert pandas NA to None
                        if pd.isna(value):
                            row_data.append(None)
                        # Convert datetime to string in Access format
                        elif isinstance(value, pd.Timestamp):
                            row_data.append(value.strftime('%m/%d/%Y'))
                        # Handle UPC/Series as string
                        elif col == 'UPC / Series':
                            row_data.append(str(value))
                        # Handle numeric values with proper decimal handling
                        elif isinstance(value, (int, float)):
                            if col in ['Projected GM %', 'Planned Volume']:
                                # Convert to float with 4 decimal places
                                row_data.append(float(f"{value:.4f}"))
                            else:
                                row_data.append(float(value))
                        # Convert other types to string if they're not basic types
                        elif not isinstance(value, (int, float, str)):
                            row_data.append(str(value))
                        else:
                            row_data.append(value)
                data.append(tuple(row_data))
            
            # Log the first row for debugging
            if data:
                self.logger.info(f"First row data to insert: {data[0]}")
            
            # Execute insert with progress bar
            total_rows = len(data)
            batch_size = 1000  # Process in batches of 1000 rows
            
            print(f"\nStarting to insert {total_rows:,} rows into {table_name}")
            print("Progress:")
            
            with tqdm(total=total_rows, unit='rows', desc='Inserting') as pbar:
                for i in range(0, total_rows, batch_size):
                    batch = data[i:i + batch_size]
                    self.cursor.executemany(query, batch)
                    self.conn.commit()
                    pbar.update(len(batch))
                    
                    # Add some additional information
                    elapsed = pbar.format_dict['elapsed']
                    rate = pbar.format_dict['rate']
                    if rate:
                        remaining = (total_rows - pbar.n) / rate
                        print(f"\rSpeed: {rate:.1f} rows/sec | "
                              f"Elapsed: {elapsed:.1f}s | "
                              f"Remaining: {remaining:.1f}s", end='')
            
            print(f"\nSuccessfully inserted {total_rows:,} rows into {table_name}")
            self.logger.info(f"Successfully inserted {total_rows:,} rows into {table_name}")
            
        except Exception as e:
            self.logger.error(f"Error inserting data into {table_name}: {e}")
            # Log the query and first row for debugging
            self.logger.error(f"Query: {query}")
            if data:
                self.logger.error(f"First row data: {data[0]}")
            raise

    def delete_specific_date(self, table_name, date_str, date_column='Time'):
        """Delete records for a specific date from a table"""
        try:
            # Convert date string to datetime to validate format
            date_obj = pd.to_datetime(date_str)
            formatted_date = date_obj.strftime('%m/%d/%Y')
            
            # First count the records to be deleted
            query = f"SELECT COUNT(*) as record_count FROM [{table_name}] WHERE [{date_column}] = #{formatted_date}#"
            result = self.execute_query(query)
            record_count = result[0]['record_count'] if result else 0
            
            if record_count == 0:
                self.logger.info(f"No records found for date {formatted_date}")
                return 0
            
            self.logger.info(f"Starting deletion of {record_count:,} records for {formatted_date}")
            
            # Delete records for the specific date
            query = f"""
                DELETE FROM [{table_name}]
                WHERE [{date_column}] = #{formatted_date}#
            """
            
            self.cursor.execute(query)
            count = self.cursor.rowcount
            self.conn.commit()
            
            self.logger.info(f"Deleted {count} records for {formatted_date}")
            return count
            
        except Exception as e:
            self.logger.error(f"Error deleting data for date {date_str}: {e}")
            raise

    def process_excel_files_for_date(self, excel_dir, table_name, date_str):
        """Process Excel files for a specific date and insert data into the database"""
        excel_dir = Path(excel_dir)
        
        # Convert date string to datetime for comparison
        target_date = pd.to_datetime(date_str)
        
        # Get all Excel files
        excel_files = list(excel_dir.glob('*.xlsx'))
        
        if not excel_files:
            self.logger.info("No Excel files found to process")
            return
        
        self.logger.info(f"Found {len(excel_files)} Excel files to process")
        
        for excel_file in excel_files:
            try:
                self.logger.info(f"Processing file: {excel_file.name}")
                
                # Read Excel file
                df = pd.read_excel(excel_file)
                
                # Convert 'Time' column to datetime if it's not already
                df['Time'] = pd.to_datetime(df['Time'])
                
                # Filter data for the specific date
                df = df[df['Time'].dt.date == target_date.date()]
                
                if len(df) == 0:
                    self.logger.info(f"No data found for date {date_str} in {excel_file.name}")
                    continue
                
                # Insert data into database
                self.insert_dataframe(table_name, df)
                self.logger.info(f"Successfully inserted {len(df)} records from {excel_file.name}")
                
            except Exception as e:
                self.logger.error(f"Error processing file {excel_file.name}: {e}")
                continue

def get_quarterly_date_input():
    """Get quarterly date input from user"""
    while True:
        try:
            year = int(input("Enter year (e.g., 2025): "))
            if 1900 <= year <= 2100:  # Reasonable year range
                break
            print("Please enter a valid year between 1900 and 2100.")
        except ValueError:
            print("Please enter a valid number.")
    
    while True:
        print("\nSelect quarter:")
        print("1. Q1 (January 1st)")
        print("2. Q2 (April 1st)")
        print("3. Q3 (July 1st)")
        print("4. Q4 (October 1st)")
        quarter = input("Enter quarter (1-4): ")
        
        if quarter in ['1', '2', '3', '4']:
            break
        print("Please enter a valid quarter (1-4).")
    
    # Map quarter to month
    quarter_to_month = {
        '1': '1',
        '2': '4',
        '3': '7',
        '4': '10'
    }
    
    return f"{quarter_to_month[quarter]}/1/{year}"

def display_menu():
    """Display the main menu options"""
    print("\nDatabase Operations Menu:")
    print("1. List all tables")
    print("2. Delete data by year")
    print("3. Delete data by quarter")
    print("4. Process Excel files")
    print("5. Process Excel files for specific year")
    print("6. Process Excel files for specific quarter")
    print("7. Exit")
    return input("Enter your choice (1-7): ")

def get_table_choice(tables):
    """Let user choose a table from the list"""
    print("\nAvailable tables:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    while True:
        try:
            choice = int(input(f"\nEnter table number (1-{len(tables)}): "))
            if 1 <= choice <= len(tables):
                return tables[choice - 1]
            print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

def get_year_input():
    """Get year input from user"""
    while True:
        try:
            year = int(input("Enter year (e.g., 2025): "))
            if 1900 <= year <= 2100:  # Reasonable year range
                return year
            print("Please enter a valid year between 1900 and 2100.")
        except ValueError:
            print("Please enter a valid number.")

# Example usage
if __name__ == "__main__":
    # Initialize the database connection
    db = AccessDB("ad_test_db.accdb")
    
    try:
        # Connect to the database
        db.connect()
        
        while True:
            choice = display_menu()
            
            if choice == "1":
                # List all tables
                tables = db.get_tables()
                print("\nAvailable tables:")
                for table in tables:
                    print(f"- {table}")
            
            elif choice == "2":
                # Delete data by year
                tables = db.get_tables()
                if not tables:
                    print("No tables found in the database.")
                    continue
                
                table_name = get_table_choice(tables)
                year = get_year_input()
                
                # Count records before deletion
                try:
                    record_count = db.count_records(table_name, year)
                    
                    if record_count == 0:
                        print(f"\nNo records found for year {year} in table {table_name}")
                        continue
                    
                    confirm = input(f"\nThere are {record_count:,} records from the year {year} in table {table_name}.\nAre you sure you want to delete them? (y/n): ")
                    if confirm.lower() == 'y':
                        db.delete_year_data(table_name, year)
                        print(f"Data for year {year} has been deleted from {table_name}")
                    else:
                        print("Operation cancelled.")
                except Exception as e:
                    print(f"Error counting records: {e}")
                    continue
            
            elif choice == "3":
                # Delete data by quarter
                tables = db.get_tables()
                if not tables:
                    print("No tables found in the database.")
                    continue
                
                table_name = get_table_choice(tables)
                date_str = get_quarterly_date_input()
                
                try:
                    count = db.delete_specific_date(table_name, date_str)
                    print(f"Deleted {count} records for {date_str}")
                except Exception as e:
                    print(f"Error deleting data: {e}")
            
            elif choice == "4":
                # Process Excel files
                current_dir = Path.cwd()
                excel_dir = current_dir / "excel_files"
                processed_dir = current_dir / "processed_files"
                
                tables = db.get_tables()
                if not tables:
                    print("No tables found in the database.")
                    continue
                
                table_name = get_table_choice(tables)
                db.process_excel_files(excel_dir, processed_dir, table_name)
            
            elif choice == "5":
                # Process Excel files for specific year
                current_dir = Path.cwd()
                excel_dir = current_dir / "excel_files"
                
                tables = db.get_tables()
                if not tables:
                    print("No tables found in the database.")
                    continue
                
                table_name = get_table_choice(tables)
                year = get_year_input()
                
                try:
                    db.process_excel_files(excel_dir, None, table_name, year)
                    print(f"Successfully processed Excel files for year {year}")
                except Exception as e:
                    print(f"Error processing Excel files: {e}")
            
            elif choice == "6":
                # Process Excel files for specific quarter
                current_dir = Path.cwd()
                excel_dir = current_dir / "excel_files"
                
                tables = db.get_tables()
                if not tables:
                    print("No tables found in the database.")
                    continue
                
                table_name = get_table_choice(tables)
                date_str = get_quarterly_date_input()
                
                try:
                    db.process_excel_files_for_date(excel_dir, table_name, date_str)
                    print(f"Successfully processed Excel files for {date_str}")
                except Exception as e:
                    print(f"Error processing Excel files: {e}")
            
            elif choice == "7":
                print("Exiting...")
                break
            
            else:
                print("Invalid choice. Please try again.")
    
    finally:
        # Always close the connection
        db.close() 