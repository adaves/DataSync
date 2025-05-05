"""
DataSync CLI - Command Line Interface for DataSync operations.
"""
from pathlib import Path
from typing import Optional, List
import sys
from datetime import datetime, date
from app.database.access_utils import list_access_tables, AccessDatabaseError, access_connection
from app.database.table_operations import get_table_info
from app.database.delete_operations import delete_data_by_date, cleanup_old_temp_tables
from app.database.date_handling import DateFilter
from app.config import (
    add_database_to_history,
    get_recent_databases,
    find_access_databases_in_directory,
    get_default_database
)
from app.utils.progress import ProgressIndicator, ProgressBar, progress_callback_factory

def clear_screen():
    """Clear the terminal screen."""
    print("\033[H\033[J", end="")

def print_header(title: str):
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(f" {title} ".center(80, "="))
    print("=" * 80 + "\n")

def get_user_input(prompt: str, valid_options: Optional[list[str]] = None) -> str:
    """
    Get user input with optional validation.
    
    Args:
        prompt: Input prompt to display
        valid_options: Optional list of valid input options
        
    Returns:
        User input string
    """
    while True:
        user_input = input(prompt).strip()
        if not valid_options or user_input in valid_options:
            return user_input
        print(f"Invalid input. Please choose from: {', '.join(valid_options)}")

def parse_date_input(date_str: str) -> DateFilter:
    """
    Parse user date input into a DateFilter object.
    
    Args:
        date_str: User input date string (YYYY or MM/DD/YYYY)
        
    Returns:
        DateFilter object
        
    Raises:
        ValueError: If date format is invalid
    """
    try:
        if len(date_str) == 4:  # Year only
            year = int(date_str)
            return DateFilter(
                start_date=date(year, 1, 1),
                end_date=date(year, 12, 31),
                is_full_date=False
            )
        else:  # Full date
            month, day, year = map(int, date_str.split('/'))
            target_date = date(year, month, day)
            return DateFilter(
                start_date=target_date,
                end_date=target_date,
                is_full_date=True
            )
    except (ValueError, IndexError):
        raise ValueError(
            "Invalid date format. Use YYYY for year or MM/DD/YYYY for specific date"
        )

def select_database() -> Optional[Path]:
    """
    Interactive database selection using hybrid approach.
    
    Returns:
        Selected database path or None if cancelled
    """
    print_header("Database Selection")
    
    # 1. Check command-line argument
    if len(sys.argv) > 1:
        db_path = Path(sys.argv[1])
        if db_path.exists():
            print(f"Using database from command line: {db_path}")
            return db_path
        else:
            print(f"Warning: Database from command line not found: {db_path}")
    
    # 2. Check current directory
    local_dbs = find_access_databases_in_directory()
    
    # 3. Get recent databases
    recent_dbs = []
    for db_str in get_recent_databases():
        db_path = Path(db_str)
        if db_path.exists():
            recent_dbs.append(db_path)
    
    # Combine options
    options = []
    
    if local_dbs:
        print("\nDatabases in current directory:")
        for i, db in enumerate(local_dbs):
            options.append(db)
            print(f"{len(options)}. {db.name}")
    
    if recent_dbs:
        # Filter out databases already shown from local directory
        unique_recent_dbs = [db for db in recent_dbs if db not in options]
        
        if unique_recent_dbs:
            print("\nRecent databases:")
            for db in unique_recent_dbs:
                options.append(db)
                print(f"{len(options)}. {db}")
    
    # Manual entry option
    print("\nOther options:")
    print(f"{len(options) + 1}. Enter database path manually")
    print(f"{len(options) + 2}. Exit")
    
    # Get user choice
    while True:
        try:
            choice = int(get_user_input("\nSelect option: "))
            if 1 <= choice <= len(options):
                # Return selected database
                return options[choice - 1]
            elif choice == len(options) + 1:
                # Manual entry
                path_str = get_user_input("Enter database path: ")
                db_path = Path(path_str)
                if db_path.exists():
                    if db_path.suffix.lower() in ['.accdb', '.mdb']:
                        return db_path
                    else:
                        print("Error: Not an Access database file (.accdb or .mdb)")
                else:
                    print("Error: File not found")
            elif choice == len(options) + 2:
                # Exit
                return None
            else:
                print(f"Please enter a number between 1 and {len(options) + 2}")
        except ValueError:
            print("Please enter a valid number")

def handle_delete_data(db_path: Path):
    """Handle the delete data workflow."""
    print_header("Delete Data by Date")
    
    # List available tables
    try:
        with ProgressIndicator("Loading tables"):
            tables = list_access_tables(db_path)
        
        if not tables:
            print("No tables found in the database.")
            return
    except AccessDatabaseError as e:
        print(f"Error accessing database: {e}")
        return
    
    # Display tables
    print("Available tables:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    # Get table selection
    while True:
        try:
            choice = int(get_user_input("\nSelect table number: "))
            if 1 <= choice <= len(tables):
                selected_table = tables[choice - 1]
                break
            print(f"Please enter a number between 1 and {len(tables)}")
        except ValueError:
            print("Please enter a valid number")
    
    # Get table structure to identify date columns
    try:
        with ProgressIndicator("Loading table structure"):
            table_info = get_table_info(db_path, selected_table)
            
        # Find date columns
        date_columns = []
        for col in table_info.columns:
            if col.data_type.lower() in ["datetime", "date", "date/time"]:
                date_columns.append(col.name)
        
        if not date_columns:
            print(f"No date columns found in table {selected_table}.")
            return
    except AccessDatabaseError as e:
        print(f"Error: {e}")
        return
    
    # Get date column selection
    print("\nAvailable date columns:")
    for i, col in enumerate(date_columns, 1):
        print(f"{i}. {col}")
    
    while True:
        try:
            choice = int(get_user_input("\nSelect date column number: "))
            if 1 <= choice <= len(date_columns):
                selected_date_column = date_columns[choice - 1]
                break
            print(f"Please enter a number between 1 and {len(date_columns)}")
        except ValueError:
            print("Please enter a valid number")
    
    # Get date input
    while True:
        date_str = get_user_input(
            "\nEnter date (YYYY for year or MM/DD/YYYY for specific date): "
        )
        try:
            date_filter = parse_date_input(date_str)
            break
        except ValueError as e:
            print(f"Error: {e}")
    
    # Confirm deletion
    date_display = (
        f"year {date_filter.start_date.year}"
        if not date_filter.is_full_date
        else date_filter.start_date.strftime("%m/%d/%Y")
    )
    confirm = get_user_input(
        f"\nAre you sure you want to delete all data for {date_display} from {selected_table} "
        f"(filtered by {selected_date_column})? (yes/no): ",
        ["yes", "no"]
    )
    
    if confirm.lower() != "yes":
        print("Operation cancelled.")
        return
    
    # Perform deletion
    try:
        progress = ProgressIndicator("Deleting data")
        progress.start()
        
        temp_table = delete_data_by_date(
            db_path=db_path,
            table_name=selected_table,
            date_filter=date_filter,
            date_column=selected_date_column
        )
        
        progress.stop()
        print(f"\nSuccess! Data has been moved to temporary table: {temp_table}")
        print("This table will be automatically cleaned up after 7 days.")
    except AccessDatabaseError as e:
        progress.stop()
        print(f"\nError: {e}")

def handle_cleanup_temp_tables(db_path: Path):
    """Handle the cleanup of old temporary tables."""
    print_header("Cleanup Old Temporary Tables")
    
    # Get days to keep
    while True:
        try:
            days_str = get_user_input(
                "Enter number of days to keep temporary tables (default: 7): "
            ) or "7"
            days = int(days_str)
            if days > 0:
                break
            print("Please enter a positive number of days")
        except ValueError:
            print("Please enter a valid number")
    
    # Confirm cleanup
    confirm = get_user_input(
        f"\nAre you sure you want to delete temporary tables older than {days} days? (yes/no): ",
        ["yes", "no"]
    )
    
    if confirm.lower() != "yes":
        print("Operation cancelled.")
        return
    
    # Perform cleanup
    try:
        progress = ProgressIndicator("Cleaning up old temporary tables")
        progress.start()
        
        deleted_tables = cleanup_old_temp_tables(db_path, days)
        
        progress.stop()
        
        if deleted_tables:
            print(f"\nSuccessfully deleted {len(deleted_tables)} temporary tables:")
            for table in deleted_tables:
                print(f"- {table}")
        else:
            print("\nNo temporary tables found to clean up.")
    except AccessDatabaseError as e:
        print(f"\nError: {e}")

def handle_show_structure(db_path: Path):
    """Handle the show table structure command."""
    print_header("Show Table Structure")
    
    # List available tables
    try:
        with ProgressIndicator("Loading tables"):
            tables = list_access_tables(db_path)
        
        if not tables:
            print("No tables found in the database.")
            return
    except AccessDatabaseError as e:
        print(f"Error accessing database: {e}")
        return
    
    # Display tables
    print("Available tables:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    # Get table selection
    while True:
        try:
            choice = int(get_user_input("\nSelect table number: "))
            if 1 <= choice <= len(tables):
                selected_table = tables[choice - 1]
                break
            print(f"Please enter a number between 1 and {len(tables)}")
        except ValueError:
            print("Please enter a valid number")
    
    # Get and display table info
    try:
        progress = ProgressIndicator("Loading table structure")
        progress.start()
        
        table_info = get_table_info(db_path, selected_table)
        
        progress.stop()
        
        print(f"\nStructure of table: {table_info.name}")
        print("-" * 80)
        print(f"{'Column Name':<30} {'Data Type':<20} {'Nullable':<10} {'Primary Key':<10}")
        print("-" * 80)
        
        for col in table_info.columns:
            nullable = "Yes" if col.is_nullable else "No"
            primary_key = "Yes" if col.is_primary_key else "No"
            size_info = f"({col.character_maximum_length})" if col.character_maximum_length else ""
            data_type = f"{col.data_type}{size_info}"
            
            print(f"{col.name:<30} {data_type:<20} {nullable:<10} {primary_key:<10}")
        
        print("-" * 80)
        print(f"Total columns: {len(table_info.columns)}")
        
    except AccessDatabaseError as e:
        progress.stop()
        print(f"\nError: {e}")

def main():
    """Main CLI entry point."""
    # Select database using hybrid approach
    db_path = select_database()
    if db_path is None:
        print("\nExiting application. Goodbye!")
        return
    
    # Add to history
    add_database_to_history(db_path)
    
    while True:
        clear_screen()
        print_header("DataSync - Main Menu")
        print(f"Current database: {db_path}")
        print("\n1. List Tables")
        print("2. Show Table Structure")
        print("3. Delete Data by Date")
        print("4. Cleanup Old Temporary Tables")
        print("5. Change Database")
        print("6. Exit")
        
        choice = get_user_input("\nSelect an option (1-6): ", ["1", "2", "3", "4", "5", "6"])
        
        if choice == "1":
            try:
                with ProgressIndicator("Loading tables"):
                    tables = list_access_tables(db_path)
                
                print("\nAvailable tables:")
                for i, table in enumerate(tables, 1):
                    print(f"{i}. {table}")
                print(f"\nTotal tables: {len(tables)}")
            except AccessDatabaseError as e:
                print(f"Error: {e}")
        elif choice == "2":
            handle_show_structure(db_path)
        elif choice == "3":
            handle_delete_data(db_path)
        elif choice == "4":
            handle_cleanup_temp_tables(db_path)
        elif choice == "5":
            # Select a different database
            new_db_path = select_database()
            if new_db_path is not None:
                db_path = new_db_path
                add_database_to_history(db_path)
                continue
        elif choice == "6":
            print("\nGoodbye!")
            break
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user. Goodbye!")
        sys.exit(0) 