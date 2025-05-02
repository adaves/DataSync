"""
DataSync CLI - Command Line Interface for DataSync operations.
"""
from pathlib import Path
from typing import Optional
import sys
from app.database.access_utils import list_access_tables, AccessDatabaseError
from app.database.table_operations import get_table_info

def clear_screen():
    """Clear the terminal screen."""
    print("\033[H\033[J", end="")

def print_header():
    """Print the application header."""
    print("=" * 50)
    print("DataSync - Access Database Management Tool")
    print("=" * 50)
    print()

def print_menu():
    """Print the main menu options."""
    print("\nAvailable Commands:")
    print("1. List tables in database")
    print("2. Show table structure")
    print("3. View/filter table data (coming soon)")
    print("4. Delete data by date (coming soon)")
    print("5. Upload Excel data (coming soon)")
    print("q. Quit")
    print()

def handle_list_tables(db_path: Path):
    """Handle the list tables command."""
    try:
        tables = list_access_tables(db_path)
        if not tables:
            print("\nNo tables found in the database.")
            return

        print("\nTables in database:")
        print("-" * 40)
        for i, table in enumerate(tables, 1):
            print(f"{i:2d}. {table}")
        print("-" * 40)
        print(f"Total tables: {len(tables)}")
        
    except AccessDatabaseError as e:
        print(f"\nError accessing database: {e}")
    except Exception as e:
        print(f"\nUnexpected error: {e}")

def handle_show_structure(db_path: Path):
    """Handle the show table structure command."""
    try:
        # First get list of tables
        tables = list_access_tables(db_path)
        if not tables:
            print("\nNo tables found in the database.")
            return

        # Show tables and let user select one
        print("\nSelect a table:")
        print("-" * 40)
        for i, table in enumerate(tables, 1):
            print(f"{i:2d}. {table}")
        print("-" * 40)
        
        while True:
            try:
                choice = input("\nEnter table number (or 'b' for back): ").lower().strip()
                if choice == 'b':
                    return
                    
                table_idx = int(choice) - 1
                if 0 <= table_idx < len(tables):
                    break
                print("Invalid table number. Please try again.")
            except ValueError:
                print("Please enter a valid number.")
        
        # Get and display table info
        table_name = tables[table_idx]
        table_info = get_table_info(db_path, table_name)
        
        print(f"\nStructure of table: {table_info.name}")
        print("-" * 60)
        print(f"{'Column Name':<20} {'Data Type':<15} {'Nullable':<10} {'Primary Key':<10}")
        print("-" * 60)
        
        for col in table_info.columns:
            print(f"{col.name:<20} {col.data_type:<15} {str(col.is_nullable):<10} {str(col.is_primary_key):<10}")
        
        print("-" * 60)
        
    except AccessDatabaseError as e:
        print(f"\nError accessing database: {e}")
    except Exception as e:
        print(f"\nUnexpected error: {e}")

def main():
    """Main CLI entry point."""
    # Default database path - can be made configurable later
    db_path = Path('docs/Database11.accdb').absolute()
    
    while True:
        clear_screen()
        print_header()
        
        # Show current database
        print(f"Current database: {db_path}")
        
        print_menu()
        
        choice = input("Enter your choice (1-5, q to quit): ").lower().strip()
        
        if choice == 'q':
            print("\nGoodbye!")
            break
            
        elif choice == '1':
            handle_list_tables(db_path)
            input("\nPress Enter to continue...")
            
        elif choice == '2':
            handle_show_structure(db_path)
            input("\nPress Enter to continue...")
            
        elif choice in ['3', '4', '5']:
            print("\nThis feature is coming soon!")
            input("\nPress Enter to continue...")
            
        else:
            print("\nInvalid choice. Please try again.")
            input("\nPress Enter to continue...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user. Goodbye!")
        sys.exit(0) 