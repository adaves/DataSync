#!/usr/bin/env python3
"""
Production Database Setup Helper
"""

import os
import sys
from pathlib import Path

def setup_production_database():
    """Help user configure their production database."""
    print("🔧 DataSync Production Database Setup")
    print("=" * 50)
    print()
    
    print("This helper will configure DataSync to use your production database.")
    print("You have three options:")
    print()
    print("1. Use command line parameter (Recommended)")
    print("2. Set environment variable")
    print("3. Update default database locations")
    print()
    
    choice = input("Select option (1-3): ").strip()
    
    if choice == "1":
        print("\n📋 Option 1: Command Line Parameter")
        print("=" * 40)
        print("Simply add --database parameter to your commands:")
        print()
        print("Example commands:")
        print('  python src\\datasync\\cli.py auto-import --database "C:\\path\\to\\your\\production.accdb"')
        print('  python src\\datasync\\cli.py status --database "C:\\path\\to\\your\\production.accdb"')
        print()
        print("This is the recommended approach as it's explicit and safe.")
        
    elif choice == "2":
        print("\n🌍 Option 2: Environment Variable")
        print("=" * 40)
        
        db_path = input("Enter the full path to your production database: ").strip().strip('"')
        
        if not os.path.exists(db_path):
            print(f"❌ Database file not found: {db_path}")
            return
        
        print("\nTo set the environment variable:")
        print("For current session:")
        print(f'  set DATASYNC_DATABASE={db_path}')
        print()
        print("For permanent setup (Windows):")
        print("1. Open System Properties > Environment Variables")
        print("2. Add new variable:")
        print("   Variable name: DATASYNC_DATABASE")
        print(f"   Variable value: {db_path}")
        print()
        print("After setting the environment variable, you can run:")
        print("  python src\\datasync\\cli.py auto-import")
        print("  python src\\datasync\\cli.py status")
        
    elif choice == "3":
        print("\n⚙️ Option 3: Update Default Locations")
        print("=" * 40)
        
        db_path = input("Enter the full path to your production database: ").strip().strip('"')
        
        if not os.path.exists(db_path):
            print(f"❌ Database file not found: {db_path}")
            return
        
        cli_file = Path("src/datasync/cli.py")
        if not cli_file.exists():
            print("❌ CLI file not found. Make sure you're running this from the project root.")
            return
        
        print("\n📝 To update default locations:")
        print(f"1. Open: {cli_file}")
        print("2. Find the DEFAULT_DB_LOCATIONS section (around line 38)")
        print("3. Add your database path as the first entry:")
        print(f'   Path("{db_path.replace(chr(92), chr(92)+chr(92))}"),')
        print()
        print("Example:")
        print("DEFAULT_DB_LOCATIONS = [")
        print(f'    Path("{db_path.replace(chr(92), chr(92)+chr(92))}"),  # Your production database')
        print('    Path.home() / "Documents" / "DataSync" / "database.accdb",')
        print('    Path.cwd() / "database.accdb",')
        print(']')
        
    else:
        print("❌ Invalid choice")
        return
    
    print()
    print("✅ Configuration help completed!")
    print()
    print("🧪 Test your setup:")
    print("  python src\\datasync\\cli.py status")
    print()

if __name__ == "__main__":
    setup_production_database()
