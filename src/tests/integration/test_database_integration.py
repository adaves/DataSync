"""
Integration tests for database operations.
This module contains tests that verify the interaction between different components
and test real database operations with actual Microsoft Access databases.
"""

import pytest
import os
from pathlib import Path
from datetime import datetime
import pyodbc
from datasync.database.operations import DatabaseOperations
from datasync.database.validation import DatabaseValidation
from datasync.database.monitoring import DatabaseMonitor

@pytest.fixture(scope="module")
def test_db_path(tmp_path_factory):
    """Create a temporary test database that persists for all tests in this module."""
    db_path = tmp_path_factory.mktemp("test_db") / "test_database.accdb"
    return db_path

@pytest.fixture(scope="module")
def db_operations(test_db_path):
    """Create a DatabaseOperations instance with the test database."""
    return DatabaseOperations(test_db_path)

@pytest.fixture(scope="module")
def db_validation():
    """Create a DatabaseValidation instance."""
    return DatabaseValidation()

@pytest.fixture(scope="module")
def db_monitor():
    """Create a DatabaseMonitor instance."""
    return DatabaseMonitor()

class TestDatabaseIntegration:
    """Test suite for database integration tests."""
    
    def test_database_connection(self, db_operations):
        """Test establishing a real database connection."""
        try:
            db_operations.connect()
            assert db_operations.conn is not None
            assert db_operations.cursor is not None
        finally:
            db_operations.close()
    
    def test_create_and_query_table(self, db_operations):
        """Test creating a table and performing basic queries."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestTable (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50),
                Value DOUBLE,
                CreatedDate DATETIME
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Verify table was created
            tables = db_operations.get_tables()
            assert "TestTable" in tables
            
            # Insert some test data
            test_data = [
                ("Test1", 42.5, datetime.now()),
                ("Test2", 99.9, datetime.now())
            ]
            
            for name, value, date in test_data:
                insert_sql = """
                INSERT INTO TestTable (Name, Value, CreatedDate)
                VALUES (?, ?, ?)
                """
                db_operations.execute_query(insert_sql, (name, value, date))
            
            # Query the data
            select_sql = "SELECT * FROM TestTable ORDER BY ID"
            results = db_operations.execute_query(select_sql)
            
            assert len(results) == 2
            assert results[0]["Name"] == "Test1"
            assert results[0]["Value"] == 42.5
            assert results[1]["Name"] == "Test2"
            assert results[1]["Value"] == 99.9
            
        finally:
            # Clean up
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestTable")
                db_operations.close()
    
    def test_transaction_rollback(self, db_operations):
        """Test transaction rollback on error."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestTransactions (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50) NOT NULL
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Start a transaction
            db_operations.execute_query("BEGIN TRANSACTION")
            
            # Insert valid data
            db_operations.execute_query(
                "INSERT INTO TestTransactions (Name) VALUES (?)",
                ("Valid",)
            )
            
            # Try to insert invalid data (should fail)
            with pytest.raises(pyodbc.Error):
                db_operations.execute_query(
                    "INSERT INTO TestTransactions (Name) VALUES (?)",
                    (None,)  # This should fail due to NOT NULL constraint
                )
            
            # Rollback the transaction
            db_operations.execute_query("ROLLBACK")
            
            # Verify no data was inserted
            results = db_operations.execute_query("SELECT * FROM TestTransactions")
            assert len(results) == 0
            
        finally:
            # Clean up
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestTransactions")
                db_operations.close()
    
    def test_concurrent_operations(self, db_operations):
        """Test handling of concurrent database operations."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestConcurrency (
                ID COUNTER PRIMARY KEY,
                Counter INT DEFAULT 0
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Insert initial data
            db_operations.execute_query(
                "INSERT INTO TestConcurrency (Counter) VALUES (0)"
            )
            
            # Simulate concurrent updates
            update_sql = """
            UPDATE TestConcurrency
            SET Counter = Counter + 1
            WHERE ID = 1
            """
            
            # Perform multiple updates
            for _ in range(5):
                db_operations.execute_query(update_sql)
            
            # Verify final state
            results = db_operations.execute_query(
                "SELECT Counter FROM TestConcurrency WHERE ID = 1"
            )
            assert results[0]["Counter"] == 5
            
        finally:
            # Clean up
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestConcurrency")
                db_operations.close()
    
    def test_error_handling(self, db_operations):
        """Test error handling with invalid operations."""
        try:
            db_operations.connect()
            
            # Test invalid table name
            with pytest.raises(pyodbc.Error):
                db_operations.execute_query("SELECT * FROM NonExistentTable")
            
            # Test invalid SQL syntax
            with pytest.raises(pyodbc.Error):
                db_operations.execute_query("INVALID SQL STATEMENT")
            
            # Test invalid parameter count
            with pytest.raises(pyodbc.Error):
                db_operations.execute_query(
                    "SELECT * FROM TestTable WHERE ID = ?",
                    (1, 2)  # Too many parameters
                )
            
        finally:
            if db_operations.conn:
                db_operations.close()

    def test_insert_record_integration(self, db_operations):
        """Test insert_record with a real database."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestInsert (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50),
                Value DOUBLE,
                CreatedDate DATETIME
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Test single record insertion
            record = {
                "Name": "Test Insert",
                "Value": 42.5,
                "CreatedDate": datetime.now()
            }
            
            result = db_operations.insert_record("TestInsert", record)
            assert result == 1
            
            # Verify the record was inserted
            results = db_operations.execute_query("SELECT * FROM TestInsert")
            assert len(results) == 1
            assert results[0]["Name"] == "Test Insert"
            assert results[0]["Value"] == 42.5
            
        finally:
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestInsert")
                db_operations.close()

    def test_batch_insert_integration(self, db_operations):
        """Test batch_insert with a real database."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestBatchInsert (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50),
                Value DOUBLE,
                CreatedDate DATETIME
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Prepare test data
            records = [
                {
                    "Name": f"Test {i}",
                    "Value": float(i),
                    "CreatedDate": datetime.now()
                }
                for i in range(1, 6)  # 5 records
            ]
            
            # Test batch insertion
            result = db_operations.batch_insert("TestBatchInsert", records, batch_size=2)
            assert result == 5
            
            # Verify all records were inserted
            results = db_operations.execute_query("SELECT * FROM TestBatchInsert ORDER BY ID")
            assert len(results) == 5
            for i, record in enumerate(results, 1):
                assert record["Name"] == f"Test {i}"
                assert record["Value"] == float(i)
            
        finally:
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestBatchInsert")
                db_operations.close()

    def test_upsert_integration(self, db_operations):
        """Test upsert with a real database."""
        try:
            db_operations.connect()
            
            # Create a test table with a unique constraint
            create_table_sql = """
            CREATE TABLE TestUpsert (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50) UNIQUE,
                Value DOUBLE,
                CreatedDate DATETIME
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Test initial insert
            record = {
                "Name": "Test Upsert",
                "Value": 42.5,
                "CreatedDate": datetime.now()
            }
            
            result = db_operations.upsert("TestUpsert", record, ["Name"])
            assert result == 1
            
            # Verify initial insert
            results = db_operations.execute_query("SELECT * FROM TestUpsert")
            assert len(results) == 1
            assert results[0]["Value"] == 42.5
            
            # Test update
            updated_record = {
                "Name": "Test Upsert",  # Same name to trigger update
                "Value": 99.9,          # Different value
                "CreatedDate": datetime.now()
            }
            
            result = db_operations.upsert("TestUpsert", updated_record, ["Name"])
            assert result == 1
            
            # Verify update
            results = db_operations.execute_query("SELECT * FROM TestUpsert")
            assert len(results) == 1
            assert results[0]["Value"] == 99.9
            
        finally:
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestUpsert")
                db_operations.close()

    def test_transaction_integration(self, db_operations):
        """Test transaction management with a real database."""
        try:
            db_operations.connect()
            
            # Create a test table
            create_table_sql = """
            CREATE TABLE TestTransactions (
                ID COUNTER PRIMARY KEY,
                Name TEXT(50),
                Value DOUBLE
            )
            """
            db_operations.execute_query(create_table_sql)
            
            # Test transaction rollback
            db_operations.begin_transaction()
            
            # Insert a record
            record = {
                "Name": "Test Transaction",
                "Value": 42.5
            }
            db_operations.insert_record("TestTransactions", record)
            
            # Verify record exists before rollback
            results = db_operations.execute_query("SELECT * FROM TestTransactions")
            assert len(results) == 1
            
            # Rollback the transaction
            db_operations.rollback_transaction()
            
            # Verify record was rolled back
            results = db_operations.execute_query("SELECT * FROM TestTransactions")
            assert len(results) == 0
            
            # Test successful transaction
            db_operations.begin_transaction()
            
            # Insert a record
            db_operations.insert_record("TestTransactions", record)
            
            # Commit the transaction
            db_operations.commit_transaction()
            
            # Verify record exists after commit
            results = db_operations.execute_query("SELECT * FROM TestTransactions")
            assert len(results) == 1
            assert results[0]["Name"] == "Test Transaction"
            assert results[0]["Value"] == 42.5
            
        finally:
            if db_operations.conn:
                db_operations.execute_query("DROP TABLE TestTransactions")
                db_operations.close()

class TestReadOperationsIntegration:
    """Test suite for read operations integration tests."""
    
    @pytest.fixture(autouse=True)
    def setup_test_data(self, db_operations):
        """Set up test data for read operation tests."""
        try:
            db_operations.connect()
            
            # Create test tables
            db_operations.execute_query("""
                CREATE TABLE test_products (
                    id INTEGER PRIMARY KEY,
                    name TEXT(50),
                    category TEXT(50),
                    price DECIMAL(10,2),
                    stock INTEGER,
                    created_date DATE
                )
            """)
            
            db_operations.execute_query("""
                CREATE TABLE test_orders (
                    id INTEGER PRIMARY KEY,
                    product_id INTEGER,
                    quantity INTEGER,
                    order_date DATE,
                    status TEXT(20)
                )
            """)
            
            # Insert test data
            products = [
                (1, "Product A", "Category 1", 10.99, 100, "2023-01-01"),
                (2, "Product B", "Category 1", 15.99, 50, "2023-01-15"),
                (3, "Product C", "Category 2", 20.99, 75, "2023-02-01"),
                (4, "Product D", "Category 2", 25.99, 30, "2023-02-15"),
                (5, "Product E", "Category 3", 30.99, 20, "2023-03-01")
            ]
            
            orders = [
                (1, 1, 5, "2023-01-10", "completed"),
                (2, 2, 3, "2023-01-20", "completed"),
                (3, 3, 2, "2023-02-05", "pending"),
                (4, 4, 1, "2023-02-10", "completed"),
                (5, 5, 4, "2023-03-05", "pending")
            ]
            
            for product in products:
                db_operations.execute_query(
                    "INSERT INTO test_products VALUES (?, ?, ?, ?, ?, ?)",
                    product
                )
            
            for order in orders:
                db_operations.execute_query(
                    "INSERT INTO test_orders VALUES (?, ?, ?, ?, ?)",
                    order
                )
            
            yield
            
        finally:
            # Clean up
            db_operations.execute_query("DROP TABLE test_products")
            db_operations.execute_query("DROP TABLE test_orders")
            db_operations.close()
    
    def test_basic_read_operations(self, db_operations):
        """Test basic read operations with real data."""
        results = db_operations.read_records("test_products")
        assert len(results) == 5
        assert results[0]["name"] == "Product A"
        assert results[0]["price"] == 10.99
    
    def test_filtering_operations(self, db_operations):
        """Test filtering operations with real data."""
        # Test basic filter
        results = db_operations.read_records(
            "test_products",
            filters={"category": "Category 1"}
        )
        assert len(results) == 2
        assert all(r["category"] == "Category 1" for r in results)
        
        # Test IN clause
        results = db_operations.read_records(
            "test_products",
            filters={"id": [1, 3, 5]}
        )
        assert len(results) == 3
        assert all(r["id"] in [1, 3, 5] for r in results)
        
        # Test date range
        date_range = {
            "column": "created_date",
            "start_date": "2023-01-01",
            "end_date": "2023-01-31"
        }
        results = db_operations.read_records(
            "test_products",
            date_range=date_range
        )
        assert len(results) == 2
        assert all("2023-01" in r["created_date"] for r in results)
    
    def test_sorting_and_pagination(self, db_operations):
        """Test sorting and pagination with real data."""
        # Test sorting
        results = db_operations.read_records(
            "test_products",
            sort_by=["price"],
            sort_desc=True
        )
        assert len(results) == 5
        assert results[0]["price"] == 30.99
        assert results[-1]["price"] == 10.99
        
        # Test pagination
        results = db_operations.read_records(
            "test_products",
            limit=2,
            offset=2
        )
        assert len(results) == 2
        assert results[0]["id"] == 3
    
    def test_aggregate_operations(self, db_operations):
        """Test aggregate operations with real data."""
        # Test basic aggregation
        aggregates = {
            "total_stock": "SUM(stock)",
            "avg_price": "AVG(price)",
            "product_count": "COUNT(*)"
        }
        results = db_operations.aggregate_query("test_products", aggregates)
        assert len(results) == 1
        assert results[0]["total_stock"] == 275
        assert round(results[0]["avg_price"], 2) == 20.79
        assert results[0]["product_count"] == 5
        
        # Test grouping
        aggregates = {
            "total_stock": "SUM(stock)",
            "product_count": "COUNT(*)"
        }
        results = db_operations.aggregate_query(
            "test_products",
            aggregates,
            group_by=["category"]
        )
        assert len(results) == 3
        category1 = next(r for r in results if r["category"] == "Category 1")
        assert category1["total_stock"] == 150
    
    def test_subquery_operations(self, db_operations):
        """Test subquery operations with real data."""
        # Test EXISTS subquery
        subquery = """
            SELECT 1 FROM test_orders 
            WHERE test_orders.product_id = test_products.id 
            AND test_orders.status = ?
        """
        results = db_operations.subquery(
            "test_products",
            subquery,
            subquery_params=("completed",)
        )
        assert len(results) == 3
        assert all(r["id"] in [1, 2, 4] for r in results)
        
        # Test subquery with additional filters
        results = db_operations.subquery(
            "test_products",
            subquery,
            subquery_params=("completed",),
            filters={"category": "Category 1"}
        )
        assert len(results) == 2
        assert all(r["category"] == "Category 1" for r in results)
    
    def test_complex_query_building(self, db_operations):
        """Test complex query building with real data."""
        query = db_operations.build_query(
            "test_products",
            columns=["id", "name", "price"],
            filters={"category": "Category 1"},
            sort_by=["price"],
            sort_desc=True
        )
        results = db_operations.execute_query(query, ("Category 1",))
        assert len(results) == 2
        assert results[0]["price"] == 15.99
        assert results[1]["price"] == 10.99

class TestUpdateOperationsIntegration:
    """Test suite for update operations integration tests."""
    
    @pytest.fixture(autouse=True)
    def setup_test_data(self, db_operations):
        """Set up test data for update operation tests."""
        try:
            db_operations.connect()
            
            # Create test tables
            db_operations.execute_query("""
                CREATE TABLE test_products (
                    id INTEGER PRIMARY KEY,
                    name TEXT(50),
                    category TEXT(50),
                    price DECIMAL(10,2),
                    stock INTEGER,
                    last_updated DATETIME
                )
            """)
            
            db_operations.execute_query("""
                CREATE TABLE test_orders (
                    id INTEGER PRIMARY KEY,
                    product_id INTEGER,
                    quantity INTEGER,
                    order_date DATE,
                    status TEXT(20)
                )
            """)
            
            # Insert test data
            products = [
                (1, "Product A", "Category 1", 10.99, 100, "2023-01-01 10:00:00"),
                (2, "Product B", "Category 1", 15.99, 50, "2023-01-15 11:00:00"),
                (3, "Product C", "Category 2", 20.99, 75, "2023-02-01 12:00:00"),
                (4, "Product D", "Category 2", 25.99, 30, "2023-02-15 13:00:00"),
                (5, "Product E", "Category 3", 30.99, 20, "2023-03-01 14:00:00")
            ]
            
            orders = [
                (1, 1, 5, "2023-01-10", "completed"),
                (2, 2, 3, "2023-01-20", "completed"),
                (3, 3, 2, "2023-02-05", "pending"),
                (4, 4, 1, "2023-02-10", "completed"),
                (5, 5, 4, "2023-03-05", "pending")
            ]
            
            for product in products:
                db_operations.execute_query(
                    "INSERT INTO test_products VALUES (?, ?, ?, ?, ?, ?)",
                    product
                )
            
            for order in orders:
                db_operations.execute_query(
                    "INSERT INTO test_orders VALUES (?, ?, ?, ?, ?)",
                    order
                )
            
            yield
            
        finally:
            # Clean up
            db_operations.execute_query("DROP TABLE test_products")
            db_operations.execute_query("DROP TABLE test_orders")
            db_operations.close()
    
    def test_single_record_update(self, db_operations):
        """Test updating a single record."""
        # Update a product
        record = {
            "id": 1,
            "name": "Updated Product A",
            "price": 12.99,
            "stock": 150
        }
        result = db_operations.update_record("test_products", record, ["id"])
        assert result == 1
        
        # Verify update
        results = db_operations.execute_query(
            "SELECT * FROM test_products WHERE id = 1"
        )
        assert len(results) == 1
        assert results[0]["name"] == "Updated Product A"
        assert results[0]["price"] == 12.99
        assert results[0]["stock"] == 150
        assert results[0]["category"] == "Category 1"  # Unchanged field
    
    def test_batch_update_products(self, db_operations):
        """Test batch updating multiple products."""
        # Prepare update records
        records = [
            {"id": 1, "price": 11.99, "stock": 200},
            {"id": 2, "price": 16.99, "stock": 100},
            {"id": 3, "price": 21.99, "stock": 150}
        ]
        
        # Update records in batch
        result = db_operations.batch_update("test_products", records, ["id"], batch_size=2)
        assert result == 3
        
        # Verify updates
        results = db_operations.execute_query(
            "SELECT * FROM test_products WHERE id IN (1, 2, 3) ORDER BY id"
        )
        assert len(results) == 3
        assert results[0]["price"] == 11.99
        assert results[0]["stock"] == 200
        assert results[1]["price"] == 16.99
        assert results[1]["stock"] == 100
        assert results[2]["price"] == 21.99
        assert results[2]["stock"] == 150
    
    def test_update_with_conditions(self, db_operations):
        """Test updating records with conditions."""
        # Update all products in Category 1
        updates = {
            "price": 14.99,
            "stock": 200
        }
        conditions = {
            "category": "Category 1"
        }
        result = db_operations.update_with_conditions("test_products", updates, conditions)
        assert result == 2
        
        # Verify updates
        results = db_operations.execute_query(
            "SELECT * FROM test_products WHERE category = 'Category 1'"
        )
        assert len(results) == 2
        assert all(r["price"] == 14.99 for r in results)
        assert all(r["stock"] == 200 for r in results)
    
    def test_update_transaction_rollback(self, db_operations):
        """Test transaction rollback during update."""
        # Start transaction
        db_operations.begin_transaction()
        
        try:
            # Update a product
            record = {
                "id": 1,
                "name": "Updated Product A",
                "price": 12.99
            }
            db_operations.update_record("test_products", record, ["id"])
            
            # Try to update with invalid data (should fail)
            with pytest.raises(pyodbc.Error):
                invalid_record = {
                    "id": 2,
                    "name": None,  # This should fail due to NOT NULL constraint
                    "price": 15.99
                }
                db_operations.update_record("test_products", invalid_record, ["id"])
            
            # Verify rollback
            results = db_operations.execute_query(
                "SELECT * FROM test_products WHERE id = 1"
            )
            assert len(results) == 1
            assert results[0]["name"] == "Product A"  # Original value
            assert results[0]["price"] == 10.99  # Original value
            
        finally:
            db_operations.rollback_transaction()
    
    def test_concurrent_updates(self, db_operations):
        """Test handling of concurrent updates."""
        # Update the same product multiple times
        for i in range(5):
            record = {
                "id": 1,
                "stock": 100 + i * 10
            }
            result = db_operations.update_record("test_products", record, ["id"])
            assert result == 1
        
        # Verify final state
        results = db_operations.execute_query(
            "SELECT * FROM test_products WHERE id = 1"
        )
        assert len(results) == 1
        assert results[0]["stock"] == 140  # 100 + (4 * 10)
    
    def test_update_with_join(self, db_operations):
        """Test updating records based on join conditions."""
        # Update product prices based on order status
        updates = {
            "price": "price * 1.1"  # 10% price increase
        }
        conditions = {
            "id": """
                SELECT DISTINCT p.id 
                FROM test_products p
                INNER JOIN test_orders o ON p.id = o.product_id
                WHERE o.status = 'completed'
            """
        }
        result = db_operations.update_with_conditions("test_products", updates, conditions)
        assert result == 3  # Products 1, 2, and 4 have completed orders
        
        # Verify updates
        results = db_operations.execute_query(
            "SELECT * FROM test_products WHERE id IN (1, 2, 4)"
        )
        assert len(results) == 3
        assert all(r["price"] > 10.99 for r in results)  # Prices should be increased

class TestDeleteOperationsIntegration:
    """Integration tests for delete operations."""
    
    @pytest.fixture(autouse=True)
    def setup(self, db_operations, temp_db_path):
        """Setup test tables and data."""
        # Create test tables
        db_operations.execute_query("""
            CREATE TABLE test_products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                status TEXT,
                deleted_at DATETIME
            )
        """)
        
        db_operations.execute_query("""
            CREATE TABLE test_orders (
                id INTEGER PRIMARY KEY,
                product_id INTEGER,
                quantity INTEGER,
                status TEXT,
                FOREIGN KEY (product_id) REFERENCES test_products(id)
            )
        """)
        
        # Insert test data
        db_operations.execute_query("""
            INSERT INTO test_products (id, name, category, status)
            VALUES (1, 'Product1', 'Electronics', 'active'),
                   (2, 'Product2', 'Electronics', 'inactive'),
                   (3, 'Product3', 'Clothing', 'active')
        """)
        
        db_operations.execute_query("""
            INSERT INTO test_orders (id, product_id, quantity, status)
            VALUES (1, 1, 2, 'pending'),
                   (2, 1, 1, 'completed'),
                   (3, 2, 3, 'pending')
        """)
        
        yield
        
        # Cleanup
        db_operations.execute_query("DROP TABLE test_orders")
        db_operations.execute_query("DROP TABLE test_products")
    
    def test_delete_with_conditions(self, db_operations):
        """Test delete with conditions in a real database scenario."""
        # Delete inactive products
        deleted_count = db_operations.delete_records("test_products", {"status": "inactive"})
        assert deleted_count == 1
        
        # Verify remaining products
        products = db_operations.execute_query("SELECT * FROM test_products")
        assert len(products) == 2
        assert all(p["status"] == "active" for p in products)
    
    def test_soft_delete_integration(self, db_operations):
        """Test soft delete in a real database scenario."""
        # Perform soft delete
        success = db_operations.soft_delete("test_products", 1)
        assert success is True
        
        # Verify product is marked as deleted
        product = db_operations.execute_query("SELECT * FROM test_products WHERE id = 1")[0]
        assert product["deleted_at"] is not None
        
        # Verify product is still in database
        products = db_operations.execute_query("SELECT * FROM test_products")
        assert len(products) == 3
    
    def test_cascade_delete_integration(self, db_operations):
        """Test cascade delete in a real database scenario."""
        # Configure cascade delete
        cascade_config = [
            {"table": "test_orders", "foreign_key": "product_id", "cascade_type": "delete"}
        ]
        
        # Perform cascade delete
        results = db_operations.cascade_delete("test_products", 1, cascade_config)
        assert results["test_products"] == 1
        assert results["test_orders"] == 2
        
        # Verify records are deleted
        products = db_operations.execute_query("SELECT * FROM test_products")
        orders = db_operations.execute_query("SELECT * FROM test_orders")
        assert len(products) == 2
        assert len(orders) == 1
        assert all(o["product_id"] != 1 for o in orders)
    
    def test_delete_with_transaction_integration(self, db_operations):
        """Test delete with transaction in a real database scenario."""
        # Start transaction
        db_operations.begin_transaction()
        
        try:
            # Delete product and its orders
            deleted_count = db_operations.delete_with_transaction("test_products", {"id": 1})
            assert deleted_count == 1
            
            # Verify product is deleted
            products = db_operations.execute_query("SELECT * FROM test_products")
            assert len(products) == 2
            
            # Commit transaction
            db_operations.commit_transaction()
            
        except Exception:
            db_operations.rollback_transaction()
            raise
    
    def test_delete_transaction_rollback_integration(self, db_operations):
        """Test transaction rollback during delete in a real database scenario."""
        # Start transaction
        db_operations.begin_transaction()
        
        try:
            # Attempt invalid delete
            db_operations.delete_with_transaction("test_products", {"invalid_column": "value"})
        except Exception:
            # Rollback should occur automatically
            pass
        
        # Verify no changes were made
        products = db_operations.execute_query("SELECT * FROM test_products")
        assert len(products) == 3
    
    def test_delete_with_related_data(self, db_operations):
        """Test delete operations with related data."""
        # First soft delete a product
        db_operations.soft_delete("test_products", 1)
        
        # Verify orders still exist
        orders = db_operations.execute_query("SELECT * FROM test_orders WHERE product_id = 1")
        assert len(orders) == 2
        
        # Then perform cascade delete
        cascade_config = [
            {"table": "test_orders", "foreign_key": "product_id", "cascade_type": "delete"}
        ]
        results = db_operations.cascade_delete("test_products", 1, cascade_config)
        
        # Verify all related data is deleted
        products = db_operations.execute_query("SELECT * FROM test_products WHERE id = 1")
        orders = db_operations.execute_query("SELECT * FROM test_orders WHERE product_id = 1")
        assert len(products) == 0
        assert len(orders) == 0 