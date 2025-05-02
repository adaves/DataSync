"""
Script to create a template Access database for testing.
"""

import os
from pathlib import Path
import pyodbc
import win32com.client

def create_template_db():
    """Create a template Access database for testing."""
    # Get the path to the template file
    template_path = Path(__file__).parent.parent / "tests" / "fixtures" / "test_db_template.accdb"
    
    # Create the database using ADOX
    catalog = win32com.client.Dispatch('ADOX.Catalog')
    catalog.Create(f"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={template_path};")
    
    print(f"Created template database at: {template_path}")

if __name__ == "__main__":
    create_template_db() 