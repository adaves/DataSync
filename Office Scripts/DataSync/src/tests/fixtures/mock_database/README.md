# Mock Database for Testing

This directory contains a mock Microsoft Access database and scripts for testing the DataSync application.

## Database Structure

The mock database contains three related tables:

### 1. Products
- ProductID (INTEGER, PRIMARY KEY)
- ProductName (TEXT)
- Category (TEXT)
- UnitPrice (CURRENCY)
- InStock (INTEGER)
- LastUpdated (DATETIME)

### 2. Orders
- OrderID (INTEGER, PRIMARY KEY)
- CustomerID (INTEGER)
- OrderDate (DATETIME)
- TotalAmount (CURRENCY)
- Status (TEXT)

### 3. OrderDetails
- OrderDetailID (INTEGER, PRIMARY KEY)
- OrderID (INTEGER, FOREIGN KEY)
- ProductID (INTEGER, FOREIGN KEY)
- Quantity (INTEGER)
- UnitPrice (CURRENCY)

## Sample Data

The database is populated with sample data:
- 5 products across 2 categories (Electronics and Furniture)
- 3 orders with different statuses
- 5 order details linking products to orders

## Usage

To create the mock database:

```bash
python create_mock_db.py
```

This will create a `mock_database.accdb` file in this directory.

## Testing

The mock database can be used in tests by copying it to a temporary location:

```python
import shutil
from pathlib import Path

def test_with_mock_db(tmp_path):
    # Copy mock database to temporary location
    mock_db = Path(__file__).parent / "mock_database.accdb"
    test_db = tmp_path / "test_db.accdb"
    shutil.copy(mock_db, test_db)
    
    # Use the database in tests
    db_ops = DatabaseOperations(test_db)
    # ... rest of the test
```

## Notes

- The mock database is designed to test various database operations including:
  - CRUD operations
  - Foreign key relationships
  - Data type handling
  - Date/time operations
  - Currency calculations 