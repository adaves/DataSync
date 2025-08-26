#!/usr/bin/env python3
"""
Time Column Diagnostic Script

This script examines the Time column in the database to understand its data type
and sample values, so we can fix the test script queries.

Usage: python diagnose_time_column.py
"""

import sys
import os

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.database.operations import DatabaseOperations

def main():
    DATABASE_PATH = "working db 8.25.2025 prodjectDataPTP.accdb"
    TARGET_TABLE = "tblProjectedDataPTP"
    
    if not os.path.exists(DATABASE_PATH):
        print(f"❌ Database file not found: {DATABASE_PATH}")
        return 1
    
    print(f"🔍 Diagnosing Time column in {TARGET_TABLE}")
    print(f"Database: {DATABASE_PATH}")
    
    try:
        # Connect to database
        db_ops = DatabaseOperations(DATABASE_PATH)
        if not db_ops.connect():
            print("❌ Failed to connect to database")
            return 1
        
        print("✅ Connected to database")
        
        # Get table schema
        print(f"\n📋 Getting table schema...")
        schema = db_ops.get_table_schema(TARGET_TABLE)
        
        # Find Time column info
        time_column_info = None
        for col in schema:
            if col['column_name'].lower() == 'time':
                time_column_info = col
                break
        
        if time_column_info:
            print(f"✅ Time column found:")
            print(f"   Column name: {time_column_info['column_name']}")
            print(f"   Data type: {time_column_info['data_type']}")
            print(f"   Size: {time_column_info.get('size', 'N/A')}")
            print(f"   Nullable: {time_column_info.get('nullable', 'N/A')}")
        else:
            print("❌ Time column not found in schema")
            print("Available columns:")
            for col in schema[:10]:  # Show first 10 columns
                print(f"   - {col['column_name']} ({col['data_type']})")
            return 1
        
        # Get sample values
        print(f"\n📊 Sample Time column values:")
        sample_query = f"SELECT TOP 10 [Time] FROM [{TARGET_TABLE}]"
        result = db_ops.execute_query(sample_query)
        
        unique_values = set()
        for i, row in enumerate(result[:10]):
            value = row[0]
            unique_values.add(str(value))
            print(f"   Row {i+1}: '{value}' (type: {type(value).__name__})")
        
        # Count distinct values
        print(f"\n📈 Distinct Time values found: {len(unique_values)}")
        if len(unique_values) <= 10:
            print("All distinct values:", sorted(unique_values))
        
        # Try to find 2025 records with different query approaches
        print(f"\n🔍 Testing different query approaches for 2025 records:")
        
        queries_to_test = [
            ("[Time] = '2025'", "String comparison"),
            ("[Time] = 2025", "Numeric comparison"), 
            ("Year([Time]) = 2025", "Year function (if date)"),
            ("[Time] LIKE '*2025*'", "Pattern matching"),
            ("[Time] LIKE '2025%'", "Starts with 2025"),
            ("[Time] LIKE '%2025%'", "Contains 2025")
        ]
        
        for where_clause, description in queries_to_test:
            try:
                query = f"SELECT COUNT(*) FROM [{TARGET_TABLE}] WHERE {where_clause}"
                result = db_ops.execute_query(query)
                count = result[0][0] if result else 0
                print(f"   ✅ {description}: {count:,} records")
                
                if count > 0:
                    # Show sample records
                    sample_query = f"SELECT TOP 3 [Time] FROM [{TARGET_TABLE}] WHERE {where_clause}"
                    sample_result = db_ops.execute_query(sample_query)
                    samples = [str(row[0]) for row in sample_result]
                    print(f"      Sample values: {', '.join(samples)}")
                    
            except Exception as e:
                print(f"   ❌ {description}: Error - {e}")
        
        db_ops.close()
        print(f"\n✅ Diagnosis complete!")
        
    except Exception as e:
        print(f"❌ Error during diagnosis: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
