"""
Script to create a mock Access database with sample tables and data for testing.
"""

import pyodbc
import pandas as pd
from pathlib import Path
import os

def create_mock_database(db_path: str):
    """
    Create a mock Access database with sample tables and data.
    
    Args:
        db_path: Path where the mock database will be created
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Create connection string
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={db_path};'
    )
    
    try:
        # Create the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Create sample tables
        create_tables(cursor)
        
        # Insert sample data
        insert_sample_data(cursor)
        
        conn.commit()
        print(f"Successfully created mock database at {db_path}")
        
    except Exception as e:
        print(f"Error creating mock database: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def create_tables(cursor):
    """Create sample tables in the mock database."""
    # Create Products table
    cursor.execute("""
        CREATE TABLE Products (
            ProductID INTEGER PRIMARY KEY,
            ProductName TEXT(50),
            Category TEXT(30),
            UnitPrice CURRENCY,
            InStock INTEGER,
            LastUpdated DATETIME
        )
    """)
    
    # Create Orders table
    cursor.execute("""
        CREATE TABLE Orders (
            OrderID INTEGER PRIMARY KEY,
            CustomerID INTEGER,
            OrderDate DATETIME,
            TotalAmount CURRENCY,
            Status TEXT(20)
        )
    """)
    
    # Create OrderDetails table
    cursor.execute("""
        CREATE TABLE OrderDetails (
            OrderDetailID INTEGER PRIMARY KEY,
            OrderID INTEGER,
            ProductID INTEGER,
            Quantity INTEGER,
            UnitPrice CURRENCY,
            FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
            FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
        )
    """)

def insert_sample_data(cursor):
    """Insert sample data into the tables."""
    # Insert sample products
    products = [
        (1, "Laptop", "Electronics", 999.99, 10, "2024-01-01"),
        (2, "Smartphone", "Electronics", 699.99, 15, "2024-01-02"),
        (3, "Headphones", "Electronics", 99.99, 20, "2024-01-03"),
        (4, "Desk", "Furniture", 199.99, 5, "2024-01-04"),
        (5, "Chair", "Furniture", 149.99, 8, "2024-01-05")
    ]
    
    cursor.executemany(
        "INSERT INTO Products (ProductID, ProductName, Category, UnitPrice, InStock, LastUpdated) VALUES (?, ?, ?, ?, ?, ?)",
        products
    )
    
    # Insert sample orders
    orders = [
        (1, 101, "2024-01-10", 1099.98, "Completed"),
        (2, 102, "2024-01-11", 849.98, "Processing"),
        (3, 103, "2024-01-12", 299.98, "Completed")
    ]
    
    cursor.executemany(
        "INSERT INTO Orders (OrderID, CustomerID, OrderDate, TotalAmount, Status) VALUES (?, ?, ?, ?, ?)",
        orders
    )
    
    # Insert sample order details
    order_details = [
        (1, 1, 1, 1, 999.99),
        (2, 1, 2, 1, 699.99),
        (3, 2, 3, 2, 99.99),
        (4, 2, 4, 1, 199.99),
        (5, 3, 5, 2, 149.99)
    ]
    
    cursor.executemany(
        "INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity, UnitPrice) VALUES (?, ?, ?, ?, ?)",
        order_details
    )

if __name__ == "__main__":
    # Create the mock database in the fixtures directory
    mock_db_path = Path(__file__).parent / "mock_database.accdb"
    create_mock_database(str(mock_db_path)) 