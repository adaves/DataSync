"""
Test script to list tables from an Access database.
"""
from pathlib import Path
from app.database.access_utils import list_access_tables, AccessDatabaseError

def main():
    # Use the actual database
    db_path = Path('docs/Database11.accdb').absolute()
    
    try:
        print(f"\nReading tables from: {db_path}\n")
        tables = list_access_tables(db_path)
        
        print("Found tables:")
        print("-" * 40)
        for i, table in enumerate(tables, 1):
            print(f"{i:2d}. {table}")
        print("-" * 40)
        print(f"Total tables found: {len(tables)}")
        
    except AccessDatabaseError as e:
        print(f"Database error: {e}")
    except FileNotFoundError as e:
        print(f"File error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()