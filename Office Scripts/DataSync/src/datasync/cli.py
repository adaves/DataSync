"""
Command-line interface for the DataSync tool.
"""

import click
import os
from pathlib import Path
import pandas as pd
from typing import Optional
# Import DatabaseOperations with proper path handling
import sys
import os

# Add the project root and src directory to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(src_dir)

# Add both paths to ensure all imports work
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Now import from the database module
from src.database.operations import DatabaseOperations
from datasync.utils.logging import setup_logger
import logging
import time
import json

# Configure logging to only write to file, not console
logger = setup_logger(__name__, logging.INFO)
logger.propagate = False  # Prevent logging from propagating to root logger

# Default database locations to check
DEFAULT_DB_LOCATIONS = [
    Path.home() / "Documents" / "DataSync" / "database.accdb",
    Path.cwd() / "database.accdb",
    Path.home() / "OneDrive" / "Documents" / "Office Scripts" / "DataSync" / "database.accdb"
]

def find_database() -> Optional[Path]:
    """Find the database file in default locations."""
    for location in DEFAULT_DB_LOCATIONS:
        if location.exists():
            return location
    return None

def get_database_path() -> Path:
    """Get the database path, prompting user if needed."""
    # Try to find database in default locations
    db_path = find_database()
    
    if db_path:
        click.echo(f"Found database at: {db_path}")
        if click.confirm("Use this database?"):
            return db_path
    
    # If no database found or user doesn't want to use it, prompt for path
    while True:
        path = click.prompt("Enter database path", type=click.Path())
        if os.path.exists(path):
            return Path(path)
        click.echo("Database file not found. Please try again.")

def get_destination_type(destination: str) -> str:
    """Determine the destination type based on file extension."""
    if destination.lower().endswith('.accdb'):
        return 'access'
    elif destination.lower().endswith('.xlsx'):
        return 'excel'
    else:
        raise click.UsageError(f"Unsupported destination type: {destination}")

@click.group()
@click.version_option(version='1.0.0')
def cli():
    """DataSync CLI - Synchronize data between Access and Excel."""
    pass

@cli.command()
@click.argument('source', type=click.Path(exists=True))
@click.argument('destination', type=click.Path())
@click.option('--batch-size', type=int, default=1000, help='Number of records to process at once')
@click.option('--validate/--no-validate', default=True, help='Validate data after synchronization')
def sync(source, destination, batch_size, validate):
    """Synchronize data from source to destination."""
    try:
        if batch_size <= 0:
            raise click.UsageError("Batch size must be greater than 0")
        
        if not os.path.exists(source):
            raise click.UsageError(f"Source file does not exist: {source}")
        
        # Create destination directory if it doesn't exist
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        
        db_ops = DatabaseOperations(destination)
        dest_type = get_destination_type(destination)
        
        if dest_type == 'access':
            db_ops.sync_to_access(source, destination, batch_size, validate)
        else:
            db_ops.sync_to_excel(source, destination, batch_size, validate)
            
        click.echo("Successfully synchronized data")
    except click.UsageError as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()

@cli.command()
@click.argument('database', type=click.Path(exists=True))
@click.option('--table', help='Specific table to validate')
@click.option('--preview/--no-preview', default=False, help='Show preview of table data')
@click.option('--preview-rows', type=int, default=5, help='Number of rows to show in preview')
def validate(database, table, preview, preview_rows):
    """Validate database structure and data."""
    try:
        if not os.path.exists(database):
            raise click.UsageError(f"Database file does not exist: {database}")
            
        db_ops = DatabaseOperations(database)
        if not db_ops.connect():
            click.echo("❌ Failed to connect to database. Run 'datasync diagnose' for troubleshooting.")
            return
        
        try:
            if table:
                # Validate specific table
                try:
                    df = db_ops.read_table(table)
                    columns = df.columns.tolist()
                    click.echo(f"Table '{table}' exists with columns: {', '.join(columns)}")
                    
                    if preview:
                        click.echo("\nPreview of data:")
                        click.echo(df.head(preview_rows).to_string())
                except Exception as e:
                    raise click.UsageError(f"Error accessing table '{table}': {str(e)}")
            else:
                # List all tables
                try:
                    tables = db_ops.get_tables()
                    if not tables:
                        click.echo("No tables found in the database")
                    else:
                        click.echo("Available tables:")
                        for table_name in tables:
                            click.echo(f"- {table_name}")
                            
                            if preview:
                                try:
                                    df = db_ops.read_table(table_name)
                                    click.echo(f"\nPreview of {table_name}:")
                                    click.echo(df.head(preview_rows).to_string())
                                    click.echo("\n" + "-"*50)
                                except Exception as e:
                                    click.echo(f"  (Could not preview {table_name}: {str(e)})")
                except Exception as e:
                    raise click.UsageError(f"Error listing tables: {str(e)}")
        finally:
            # Only close the connection after all operations are complete
            db_ops.close()
            
    except click.UsageError as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()

@cli.command()
def diagnose():
    """Diagnose ODBC driver setup and system configuration."""
    click.echo("=== DataSync System Diagnostics ===")
    click.echo()
    
    # Check ODBC drivers
    click.echo("Checking ODBC drivers...")
    driver_info = DatabaseOperations.check_odbc_drivers()
    
    if 'error' in driver_info:
        click.echo(f"❌ Error checking drivers: {driver_info['error']}")
    else:
        click.echo(f"✓ Total ODBC drivers found: {driver_info['all_drivers_count']}")
        
        if driver_info['has_access_driver']:
            click.echo("✓ Microsoft Access drivers found:")
            for driver in driver_info['access_drivers']:
                click.echo(f"  - {driver}")
        else:
            click.echo("❌ No Microsoft Access drivers found")
            click.echo()
            click.echo("To fix this issue:")
            click.echo("1. Install Microsoft Access Database Engine (64-bit)")
            click.echo("   Download: https://www.microsoft.com/en-us/download/details.aspx?id=54920")
            click.echo("2. Or install Microsoft Office with Access")
            click.echo("3. Ensure you're using the same architecture (32-bit/64-bit) for Python and the driver")
    
    click.echo()
    
    # Check Python environment
    import sys
    click.echo(f"Python version: {sys.version}")
    click.echo(f"Python architecture: {sys.maxsize > 2**32 and '64-bit' or '32-bit'}")
    
    # Check pyodbc
    try:
        import pyodbc
        click.echo(f"✓ pyodbc version: {pyodbc.version}")
    except ImportError:
        click.echo("❌ pyodbc not installed")
    
    click.echo()
    click.echo("=== End Diagnostics ===")

@cli.command()
def menu():
    """Start the interactive menu mode."""
    click.clear()
    db_path = None
    
    def show_main_menu():
        click.echo("\n=== DataSync Main Menu ===")
        click.echo("1. Database Operations")
        click.echo("2. Exit")
        return click.prompt("\nEnter your choice (1-2)", type=int)
    
    def show_database_menu():
        click.echo("\n=== Database Operations Menu ===")
        click.echo("1. View Tables")
        click.echo("2. Add Records")
        click.echo("3. Delete Records")
        click.echo("4. Recover Deleted Records")
        click.echo("5. Back to Main Menu")
        return click.prompt("\nEnter your choice (1-5)", type=int)
    
    def show_table_menu():
        click.echo("\n=== Table Operations Menu ===")
        click.echo("1. View more rows")
        click.echo("2. Export to CSV")
        click.echo("3. Back to Database Menu")
        return click.prompt("\nSelect operation (1-3)", type=int)
    
    while True:
        choice = show_main_menu()
        
        if choice == 1:  # Database Operations
            if not db_path:
                db_path = get_database_path()
            
            while True:
                db_choice = show_database_menu()
                
                if db_choice == 1:  # View Tables
                    # Connect to database and get tables
                    db_ops = DatabaseOperations(str(db_path))
                    if not db_ops.connect():
                        click.echo("❌ Failed to connect to database. Run 'datasync diagnose' for troubleshooting.")
                        continue
                    
                    try:
                        # Get list of tables
                        tables = db_ops.get_tables()
                        if not tables:
                            click.echo("No tables found in the database")
                            continue
                        
                        # Display tables in a clean format
                        click.echo("\nAvailable Tables:")
                        for i, table in enumerate(tables, 1):
                            click.echo(f"{i}. {table}")
                        
                        # Let user select a table
                        table_choice = click.prompt("\nSelect a table (number) or 0 to go back", type=int)
                        if table_choice == 0:
                            continue
                        
                        if 1 <= table_choice <= len(tables):
                            selected_table = tables[table_choice - 1]
                            
                            # Get table info
                            df = db_ops.read_table(selected_table)
                            rows, cols = df.shape
                            
                            click.echo(f"\nTable: {selected_table}")
                            click.echo(f"Shape: {rows} rows, {cols} columns")
                            
                            # Show column names
                            click.echo("\nColumns:")
                            for col in df.columns:
                                click.echo(f"- {col}")
                            
                            # Show preview
                            if click.confirm("\nShow preview of first 5 rows?"):
                                click.echo("\nPreview:")
                                click.echo(df.head().to_string())
                                
                            # Table operations submenu
                            while True:
                                op_choice = show_table_menu()
                                
                                if op_choice == 1:
                                    rows = click.prompt("Number of rows to view", type=int, default=5)
                                    click.echo(df.head(rows).to_string())
                                    
                                elif op_choice == 2:
                                    filename = click.prompt("Enter filename (without extension)", type=str)
                                    filename = f"{filename}.csv"
                                    df.to_csv(filename, index=False)
                                    click.echo(f"Data exported to {filename}")
                                    
                                elif op_choice == 3:
                                    break
                                    
                                else:
                                    click.echo("Invalid choice. Please try again.")
                        
                        else:
                            click.echo("Invalid table selection")
                            
                    except Exception as e:
                        click.echo(f"Error: {str(e)}")
                    finally:
                        db_ops.close()
                
                elif db_choice == 2:  # Add Records
                    db_ops = DatabaseOperations(str(db_path))
                    if not db_ops.connect():
                        click.echo("❌ Failed to connect to database. Run 'datasync diagnose' for troubleshooting.")
                        continue
                    
                    try:
                        tables = db_ops.get_tables()
                        if not tables:
                            click.echo("No tables found in the database")
                            continue
                        
                        click.echo("\nAvailable Tables:")
                        for i, table in enumerate(tables, 1):
                            click.echo(f"{i}. {table}")
                        
                        # Get target table (where records will be added)
                        target_choice = click.prompt("\nSelect target table to add records to (number) or 0 to go back", type=int)
                        if target_choice == 0:
                            continue
                        
                        if 1 <= target_choice <= len(tables):
                            target_table = tables[target_choice - 1]
                            
                            # Show data source options
                            click.echo("\nSelect data source:")
                            click.echo("1. Import from Excel file")
                            click.echo("2. Copy from another database table")
                            click.echo("0. Go back")
                            
                            source_type = click.prompt("\nEnter your choice", type=int)
                            if source_type == 0:
                                continue
                            elif source_type == 1:
                                # Import from Excel file
                                excel_files = [f for f in os.listdir('.') if f.endswith(('.xlsx', '.xls'))]
                                if not excel_files:
                                    click.echo("No Excel files found in the current directory")
                                    continue
                                
                                click.echo("\nAvailable Excel files:")
                                for i, file in enumerate(excel_files, 1):
                                    click.echo(f"{i}. {file}")
                                click.echo(f"{len(excel_files) + 1}. Import ALL files")
                                
                                file_choice = click.prompt("\nSelect Excel file (number), ALL files, or 0 to go back", type=int)
                                if file_choice == 0:
                                    continue
                                
                                # Handle "Import ALL files" option
                                if file_choice == len(excel_files) + 1:
                                    selected_files = excel_files
                                elif 1 <= file_choice <= len(excel_files):
                                    selected_files = [excel_files[file_choice - 1]]
                                else:
                                    click.echo("Invalid file selection")
                                    continue
                                
                                # Process all selected files
                                total_imported = 0
                                total_errors = 0
                                
                                for file_num, excel_file in enumerate(selected_files, 1):
                                    if len(selected_files) > 1:
                                        click.echo(f"\n=== Processing file {file_num}/{len(selected_files)}: {excel_file} ===")
                                    else:
                                        click.echo(f"\n=== Processing: {excel_file} ===")
                                    
                                    
                                    try:
                                        from datasync.processing.excel_processor import ExcelProcessor
                                        processor = ExcelProcessor(excel_file)
                                        
                                        # Get sheet names
                                        sheet_names = processor.get_sheet_names()
                                        if len(sheet_names) == 1:
                                            sheet_name = sheet_names[0]
                                        else:
                                            if len(selected_files) > 1:
                                                # For multiple files, use first sheet automatically
                                                sheet_name = sheet_names[0]
                                                click.echo(f"Using first sheet: {sheet_name}")
                                            else:
                                                # For single file, let user choose
                                                click.echo("\nAvailable sheets:")
                                                for i, sheet in enumerate(sheet_names, 1):
                                                    click.echo(f"{i}. {sheet}")
                                                
                                                sheet_choice = click.prompt("\nSelect sheet (number) or 0 to go back", type=int)
                                                if sheet_choice == 0:
                                                    break  # Exit the file processing loop
                                                if 1 <= sheet_choice <= len(sheet_names):
                                                    sheet_name = sheet_names[sheet_choice - 1]
                                                else:
                                                    click.echo("Invalid sheet selection")
                                                    continue
                                        
                                        # Read Excel data
                                        excel_data = processor.read_sheet(sheet_name)
                                        click.echo(f"\nFound {len(excel_data)} rows in Excel file")
                                        click.echo(f"Columns: {', '.join(excel_data.columns)}")
                                        
                                        # Get database table schema for data conversion
                                        try:
                                            table_schema = db_ops.get_table_schema(target_table)
                                            db_columns = [col['column_name'] for col in table_schema]
                                            excel_columns = list(excel_data.columns)
                                            
                                            # Show column mapping info
                                            click.echo(f"\nDatabase table columns: {', '.join(db_columns)}")
                                            matching_columns = [col for col in excel_columns if col in db_columns]
                                            click.echo(f"Matching columns: {', '.join(matching_columns)}")
                                            
                                            if not matching_columns:
                                                click.echo("❌ No matching columns found between Excel and database table!")
                                                continue
                                                
                                        except Exception as e:
                                            click.echo(f"Warning: Could not get table schema: {e}")
                                            table_schema = []
                                        
                                        # Confirm import (only ask once for multiple files)
                                        if len(selected_files) > 1:
                                            if file_num == 1:
                                                # Ask confirmation only for the first file when processing multiple
                                                if not click.confirm(f"\nImport data from ALL {len(selected_files)} Excel files into {target_table}?"):
                                                    click.echo("Import cancelled for all files")
                                                    break
                                            # For subsequent files, just show processing message
                                            confirm_import = True
                                        else:
                                            # Single file - ask for confirmation
                                            confirm_import = click.confirm(f"\nImport {len(excel_data)} records from {excel_file} into {target_table}?")
                                        
                                        if confirm_import:
                                            # Convert DataFrame to records and prepare for batch import
                                            click.echo("Processing and converting data...")
                                            converted_records = []
                                            error_count = 0
                                            
                                            for row_num, (_, row) in enumerate(excel_data.iterrows(), 1):
                                                try:
                                                    # Convert to dictionary and handle data types
                                                    record_dict = row.to_dict()
                                                    
                                                    # Apply data type conversion if schema is available
                                                    if table_schema:
                                                        record_dict = db_ops.convert_data_for_access(record_dict, table_schema)
                                                    else:
                                                        # Basic cleanup - remove NaN values
                                                        record_dict = {k: v for k, v in record_dict.items() if pd.notna(v)}
                                                    
                                                    # Skip empty records
                                                    if record_dict:
                                                        converted_records.append(record_dict)
                                                        
                                                except Exception as e:
                                                    error_count += 1
                                                    if error_count <= 5:  # Only show first 5 errors
                                                        click.echo(f"Warning: Could not process row {row_num}: {e}")
                                                    elif error_count == 6:
                                                        click.echo("... (suppressing further conversion warnings)")
                                            
                                            if converted_records:
                                                try:
                                                    click.echo(f"Inserting {len(converted_records)} records into database...")
                                                    imported_count = db_ops.insert_records_batch(target_table, converted_records, batch_size=1000)
                                                    click.echo(f"✅ Successfully imported {imported_count} records from {excel_file}")
                                                    
                                                    total_imported += imported_count
                                                    total_errors += error_count
                                                    
                                                except Exception as e:
                                                    click.echo(f"❌ Database import failed for {excel_file}: {e}")
                                                    total_errors += len(converted_records)
                                            else:
                                                click.echo(f"❌ No valid records to import from {excel_file} after data conversion")
                                                
                                            if error_count > 0:
                                                click.echo(f"⚠️  {error_count} records had conversion issues in {excel_file}")
                                        else:
                                            if len(selected_files) == 1:
                                                click.echo("Import cancelled")
                                            break  # Exit the loop if user cancels for multiple files
                                            
                                    except Exception as e:
                                        click.echo(f"❌ Error processing Excel file {excel_file}: {e}")
                                        total_errors += 1
                                
                                # Show final summary for multiple files
                                if len(selected_files) > 1:
                                    click.echo(f"\n=== IMPORT SUMMARY ===")
                                    click.echo(f"Files processed: {len(selected_files)}")
                                    click.echo(f"Total records imported: {total_imported:,}")
                                    if total_errors > 0:
                                        click.echo(f"Total conversion issues: {total_errors:,}")
                                    
                                    # Show final database count
                                    try:
                                        verification_df = db_ops.read_table(target_table)
                                        click.echo(f"📊 Database now contains {len(verification_df):,} total records in {target_table}")
                                    except Exception as e:
                                        click.echo(f"Could not verify final database count: {e}")
                                else:
                                    # For single file, show verification (if not already shown due to cancellation)
                                    if total_imported > 0:
                                        try:
                                            verification_df = db_ops.read_table(target_table)
                                            click.echo(f"📊 Database now contains {len(verification_df):,} total records in {target_table}")
                                        except Exception as e:
                                            click.echo(f"Could not verify database count: {e}")
                                    
                            elif source_type == 2:
                                # Copy from another database table (original functionality)
                                source_choice = click.prompt("\nSelect source table to copy records from (number) or 0 to go back", type=int)
                                if source_choice == 0:
                                    continue
                                
                                if 1 <= source_choice <= len(tables):
                                    source_table = tables[source_choice - 1]
                                
                                # Get table structures
                                target_df = db_ops.read_table(target_table)
                                source_df = db_ops.read_table(source_table)
                                
                                # Show available columns for filtering
                                click.echo("\nAvailable columns in source table for filtering:")
                                for col in source_df.columns:
                                    click.echo(f"- {col}")
                                
                                # Get filter column
                                filter_col = click.prompt("\nEnter column name to filter by")
                                if filter_col not in source_df.columns:
                                    click.echo("Invalid column name")
                                    continue
                                
                                # Check if column is a date type
                                is_date = False
                                if source_df[filter_col].dtype == 'datetime64[ns]' or any(isinstance(x, pd.Timestamp) for x in source_df[filter_col].head()):
                                    is_date = True
                                    click.echo(f"\n{filter_col} appears to be a date column. Please enter date in YYYY-MM-DD format or just the year (YYYY).")
                                
                                filter_value = click.prompt(f"Enter value to filter {filter_col} by")
                                
                                # Filter the source data
                                if is_date:
                                    if len(filter_value) == 4:  # Just the year
                                        filtered_df = source_df[source_df[filter_col].dt.year == int(filter_value)]
                                    else:  # Full date
                                        filtered_df = source_df[source_df[filter_col].astype(str).str.startswith(filter_value)]
                                else:
                                    filtered_df = source_df[source_df[filter_col].astype(str).str.contains(filter_value)]
                                
                                if filtered_df.empty:
                                    click.echo(f"No records found matching {filter_col} = {filter_value}")
                                    continue
                                
                                # Show matching records
                                click.echo(f"\nFound {len(filtered_df)} matching records:")
                                click.echo(filtered_df.head().to_string())
                                
                                # Map columns between source and target
                                click.echo("\nColumn mapping:")
                                column_map = {}
                                for target_col in target_df.columns:
                                    if target_col in source_df.columns:
                                        column_map[target_col] = target_col
                                        click.echo(f"{target_col} -> {target_col} (auto-mapped)")
                                    else:
                                        available_cols = [c for c in source_df.columns if c not in target_df.columns]
                                        if available_cols:
                                            click.echo(f"\nAvailable columns to map to {target_col}:")
                                            for i, avail_col in enumerate(available_cols, 1):
                                                click.echo(f"{i}. {avail_col}")
                                            map_choice = click.prompt(f"Select column to map to {target_col} (number) or 0 to skip", type=int)
                                            if 1 <= map_choice <= len(available_cols):
                                                column_map[target_col] = available_cols[map_choice - 1]
                                
                                # Confirm and proceed with import
                                if click.confirm(f"\nProceed with importing {len(filtered_df)} records?"):
                                    try:
                                        # Prepare data for import
                                        import_data = filtered_df.copy()
                                        for target_col, source_col in column_map.items():
                                            if source_col in import_data.columns:
                                                import_data[target_col] = import_data[source_col]
                                        
                                        # Keep only the columns that exist in target table
                                        import_data = import_data[target_df.columns]
                                        
                                        # Add records
                                        added_count = 0
                                        for _, row in import_data.iterrows():
                                            try:
                                                # Convert row to dictionary
                                                record = row.to_dict()
                                                # Add record to target table
                                                db_ops.add_record(target_table, record)
                                                added_count += 1
                                            except Exception as e:
                                                click.echo(f"Error adding record: {str(e)}")
                                        
                                        click.echo(f"Successfully added {added_count} records to {target_table}")
                                    
                                    except Exception as e:
                                        click.echo(f"Error during import: {str(e)}")
                            
                            else:
                                click.echo("Invalid source table selection")
                        
                        else:
                            click.echo("Invalid target table selection")
                    
                    except Exception as e:
                        click.echo(f"Error: {str(e)}")
                    finally:
                        db_ops.close()
                
                elif db_choice == 3:  # Delete Records
                    db_ops = DatabaseOperations(str(db_path))
                    if not db_ops.connect():
                        click.echo("❌ Failed to connect to database. Run 'datasync diagnose' for troubleshooting.")
                        continue
                    
                    try:
                        tables = db_ops.get_tables()
                        if not tables:
                            click.echo("No tables found in the database")
                            continue
                        
                        click.echo("\nAvailable Tables:")
                        for i, table in enumerate(tables, 1):
                            click.echo(f"{i}. {table}")
                        
                        table_choice = click.prompt("\nSelect a table to delete records from (number) or 0 to go back", type=int)
                        if table_choice == 0:
                            continue
                        
                        if 1 <= table_choice <= len(tables):
                            selected_table = tables[table_choice - 1]
                            
                            # Get table structure
                            df = db_ops.read_table(selected_table)
                            columns = df.columns.tolist()
                            
                            click.echo(f"\nDeleting records from table: {selected_table}")
                            click.echo("Available columns for filtering:")
                            for col in columns:
                                click.echo(f"- {col}")
                            
                            # Show preview of data
                            if click.confirm("\nShow preview of current data?"):
                                click.echo(df.head().to_string())
                            
                            # Get filter condition
                            filter_col = click.prompt("\nEnter column name to filter by")
                            if filter_col not in columns:
                                click.echo("Invalid column name")
                                continue
                            
                            # Check if column is a date type
                            is_date = False
                            if df[filter_col].dtype == 'datetime64[ns]' or any(isinstance(x, pd.Timestamp) for x in df[filter_col].head()):
                                is_date = True
                                click.echo(f"\n{filter_col} appears to be a date column. Please enter date in YYYY-MM-DD format.")
                            
                            filter_value = click.prompt(f"Enter value to filter {filter_col} by")
                            
                            # Build the where clause
                            if is_date:
                                where_clause = f"#{filter_value}#"  # Access date format
                            else:
                                where_clause = f"'{filter_value}'"  # String format
                            
                            # Show matching records before deletion
                            try:
                                # First get all records
                                all_records = db_ops.read_table(selected_table)
                                # Then filter in pandas
                                matching_records = all_records[all_records[filter_col].astype(str).str.contains(filter_value)]
                                
                                if matching_records.empty:
                                    click.echo(f"No records found matching {filter_col} = {filter_value}")
                                    continue
                                
                                click.echo(f"\nFound {len(matching_records)} matching records:")
                                click.echo(matching_records.head().to_string())
                                
                                # Get user name for audit trail
                                user_name = click.prompt("\nPlease enter your name for the deletion record")
                                
                                # Confirm deletion with clear warning
                                click.echo("\n" + "!" * 60)
                                click.echo("WARNING: This operation cannot be easily undone!")
                                click.echo(f"You are about to delete {len(matching_records)} records from {selected_table}")
                                click.echo("!" * 60)
                                
                                if click.confirm("\nAre you ABSOLUTELY SURE you want to delete these records?"):
                                    try:
                                        # Delete records one by one using the primary key
                                        primary_key = matching_records.columns[0]  # Assuming first column is primary key
                                        deleted_count = 0
                                        
                                        deletion_name = f"Manual deletion by {user_name}"
                                        
                                        for _, row in matching_records.iterrows():
                                            try:
                                                db_ops.delete_records(
                                                    selected_table, 
                                                    f"{primary_key} = {row[primary_key]}",
                                                    deletion_name=deletion_name
                                                )
                                                deleted_count += 1
                                            except Exception as e:
                                                click.echo(f"Error deleting record {row[primary_key]}: {str(e)}")
                                        
                                        click.echo(f"\nSuccessfully deleted {deleted_count} records")
                                        click.echo(f"Deletion recorded as: '{deletion_name}'")
                                        
                                        # Inform about recovery option
                                        click.echo("\nNOTE: If this deletion was a mistake, these records can be recovered")
                                        click.echo("      using the deleted_records table and the recovery functionality.")
                                    except Exception as e:
                                        click.echo(f"Error deleting records: {str(e)}")
                            except Exception as e:
                                click.echo(f"Error checking matching records: {str(e)}")
                    
                    except Exception as e:
                        click.echo(f"Error: {str(e)}")
                    finally:
                        db_ops.close()
                
                elif db_choice == 4:  # Recover Deleted Records
                    db_ops = DatabaseOperations(str(db_path))
                    if not db_ops.connect():
                        click.echo("❌ Failed to connect to database. Run 'datasync diagnose' for troubleshooting.")
                        continue
                    
                    try:
                        tables = db_ops.get_tables()
                        if not tables:
                            click.echo("No tables found in the database")
                            continue
                        
                        click.echo("\nAvailable Tables:")
                        for i, table in enumerate(tables, 1):
                            click.echo(f"{i}. {table}")
                        
                        table_choice = click.prompt("\nSelect a table to recover deleted records from (number) or 0 to go back", type=int)
                        if table_choice == 0:
                            continue
                        
                        if 1 <= table_choice <= len(tables):
                            selected_table = tables[table_choice - 1]
                            
                            # Get table structure
                            df = db_ops.read_table(selected_table)
                            columns = df.columns.tolist()
                            
                            click.echo(f"\nRecovering deleted records from table: {selected_table}")
                            click.echo("Available columns for filtering:")
                            for col in columns:
                                click.echo(f"- {col}")
                            
                            # Get filter condition
                            filter_col = click.prompt("\nEnter column name to filter by")
                            if filter_col not in columns:
                                click.echo("Invalid column name")
                                continue
                            
                            # Check if column is a date type
                            is_date = False
                            if df[filter_col].dtype == 'datetime64[ns]' or any(isinstance(x, pd.Timestamp) for x in df[filter_col].head()):
                                is_date = True
                                click.echo(f"\n{filter_col} appears to be a date column. Please enter date in YYYY-MM-DD format.")
                            
                            filter_value = click.prompt(f"Enter value to filter {filter_col} by")
                            
                            # Build the where clause
                            if is_date:
                                where_clause = f"#{filter_value}#"  # Access date format
                            else:
                                where_clause = f"'{filter_value}'"  # String format
                            
                            # Show matching records before recovery
                            try:
                                # First get all records
                                all_records = db_ops.read_table(selected_table)
                                # Then filter in pandas
                                matching_records = all_records[all_records[filter_col].astype(str).str.contains(filter_value)]
                                
                                if matching_records.empty:
                                    click.echo(f"No records found matching {filter_col} = {filter_value}")
                                    continue
                                
                                click.echo(f"\nFound {len(matching_records)} matching records:")
                                click.echo(matching_records.head().to_string())
                                
                                # Get user name for audit trail
                                user_name = click.prompt("\nPlease enter your name for the recovery record")
                                
                                # Confirm recovery with clear warning
                                click.echo("\n" + "!" * 60)
                                click.echo("WARNING: This operation will restore previously deleted records!")
                                click.echo(f"You are about to search for deleted records from table: {selected_table}")
                                click.echo("!" * 60)
                                
                                if click.confirm("\nProceed with searching for deleted records?"):
                                    try:
                                        # List deleted records for the selected table
                                        deleted_records = db_ops.list_deleted_records(source_table=selected_table)
                                        
                                        if not deleted_records:
                                            click.echo("\nNo deleted records found for this table.")
                                            continue
                                            
                                        click.echo(f"\nFound {len(deleted_records)} deleted records for table {selected_table}:")
                                        
                                        for i, record in enumerate(deleted_records[:10], 1):  # Show first 10
                                            deletion_time = record["deletion_timestamp"]
                                            deletion_name = record["deletion_name"]
                                            click.echo(f"{i}. ID: {record['deletion_id']} - Deleted at: {deletion_time} - By: {deletion_name}")
                                        
                                        if len(deleted_records) > 10:
                                            click.echo(f"...and {len(deleted_records) - 10} more.")
                                        
                                        # Let user select which deletion ID to recover
                                        deletion_id = click.prompt("\nEnter deletion ID to recover (or 0 to cancel)", type=int)
                                        
                                        if deletion_id == 0:
                                            continue
                                            
                                        # Confirm recovery with clear warning
                                        click.echo("\n" + "!" * 60)
                                        click.echo("WARNING: You are about to restore deleted data!")
                                        click.echo(f"This will recover records for deletion ID: {deletion_id}")
                                        click.echo("!" * 60)
                                        
                                        if click.confirm("\nAre you ABSOLUTELY SURE you want to recover these records?"):
                                            try:
                                                # Recover the records
                                                recovered = db_ops.recover_records({"deletion_id": deletion_id})
                                                
                                                if recovered > 0:
                                                    click.echo(f"\nSuccessfully recovered {recovered} records!")
                                                else:
                                                    click.echo("\nNo records were recovered.")
                                                    
                                            except Exception as e:
                                                click.echo(f"Error recovering records: {str(e)}")
                                    except Exception as e:
                                        click.echo(f"Error listing deleted records: {str(e)}")
                            except Exception as e:
                                click.echo(f"Error checking matching records: {str(e)}")
                        
                        else:
                            click.echo("Invalid table selection")
                    
                    except Exception as e:
                        click.echo(f"Error: {str(e)}")
                    finally:
                        db_ops.close()
                
                elif db_choice == 5:  # Back to Main Menu
                    break
                
                else:
                    click.echo("Invalid choice. Please try again.")
            
        elif choice == 2:  # Exit
            click.echo("Exiting DataSync. Goodbye!")
            break
            
        else:
            click.echo("Invalid choice. Please try again.")
        
        click.pause("\nPress any key to continue...")

def main():
    """Execute the CLI."""
    cli()
    
if __name__ == "__main__":
    main() 