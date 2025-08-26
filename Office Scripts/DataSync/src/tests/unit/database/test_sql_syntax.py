"""Unit tests for SQL syntax conversion utilities."""

import pytest
from datasync.database.sql_syntax import AccessSQLSyntax

def test_escape_identifier_with_reserved_words():
    """Test escaping of identifiers that are reserved words."""
    test_cases = [
        ('Time', '[Time]'),
        ('ID', '[ID]'),
        ('Count', '[Count]'),
        ('Format', '[Format]'),
        ('Update', '[Update]'),
        ('normal_column', 'normal_column'),
        ('column with space', '[column with space]'),
        ('already[bracketed]', 'already[bracketed]'),
        ('table.column', '[table.column]'),
        ('', ''),
        (None, None),
    ]
    
    for input_id, expected in test_cases:
        assert AccessSQLSyntax.escape_identifier(input_id) == expected

def test_escape_identifiers_in_query():
    """Test escaping of identifiers in complete SQL queries."""
    test_cases = [
        (
            "SELECT Count, Time, ID FROM TableName WHERE Format = 'test'",
            "SELECT [Count], [Time], [ID] FROM TableName WHERE [Format] = 'test'"
        ),
        (
            "UPDATE TableName SET Time = '2025-01-01' WHERE ID = 1",
            "UPDATE TableName SET [Time] = '2025-01-01' WHERE [ID] = 1"
        ),
        (
            "INSERT INTO TableName (Time, Count, Format) VALUES (?, ?, ?)",
            "INSERT INTO TableName ([Time], [Count], [Format]) VALUES (?, ?, ?)"
        ),
        (
            "DELETE FROM TableName WHERE ID = 1 AND Time > '2025-01-01'",
            "DELETE FROM TableName WHERE [ID] = 1 AND [Time] > '2025-01-01'"
        ),
    ]
    
    for input_query, expected in test_cases:
        result = AccessSQLSyntax.escape_identifiers_in_query(input_query)
        print(f"Input: {input_query}")
        print(f"Expected: {expected}")
        print(f"Got: {result}")
        assert result == expected

def test_create_table_with_reserved_words():
    """Test CREATE TABLE statement generation with reserved word columns."""
    columns = {
        'Time': 'DATETIME',
        'ID': 'INTEGER',
        'Count': 'INTEGER',
        'Format': 'VARCHAR',
        'normal_column': 'TEXT'
    }
    primary_key = ['ID']
    
    expected = (
        "CREATE TABLE [TestTable] (\n"
        "    [Time] DATETIME,\n"
        "    [ID] INT,\n"
        "    [Count] INT,\n"
        "    [Format] VARCHAR(255),\n"
        "    normal_column VARCHAR(255),\n"
        "    PRIMARY KEY ([ID])\n"
        ")"
    )
    
    result = AccessSQLSyntax.convert_create_table('TestTable', columns, primary_key)
    assert result == expected

def test_alter_table_with_reserved_words():
    """Test ALTER TABLE statement generation with reserved word columns."""
    add_columns = {
        'Time': 'DATETIME',
        'Count': 'INTEGER',
        'normal_column': 'TEXT'
    }
    drop_columns = ['ID', 'Format']
    
    expected = (
        "ALTER TABLE [TestTable] ADD COLUMN [Time] DATETIME;\n"
        "ALTER TABLE [TestTable] ADD COLUMN [Count] INT;\n"
        "ALTER TABLE [TestTable] ADD COLUMN normal_column VARCHAR(255);\n"
        "ALTER TABLE [TestTable] DROP COLUMN [ID];\n"
        "ALTER TABLE [TestTable] DROP COLUMN [Format]"
    )
    
    result = AccessSQLSyntax.convert_alter_table('TestTable', add_columns, drop_columns)
    assert result == expected

def test_type_mapping():
    """Test SQL type mapping to Access types."""
    test_cases = [
        ('TEXT', 'VARCHAR(255)'),
        ('INTEGER', 'INT'),
        ('DECIMAL', 'CURRENCY'),
        ('DATETIME', 'DATETIME'),
        ('BOOLEAN', 'YESNO'),
        ('BLOB', 'LONGBINARY'),
        ('COUNTER', 'AUTOINCREMENT'),
        ('CUSTOM_TYPE', 'CUSTOM_TYPE'),  # Unknown types should pass through unchanged
    ]
    
    columns = {input_type: input_type for input_type, _ in test_cases}
    result = AccessSQLSyntax.convert_create_table('TestTable', columns)
    
    for input_type, expected_type in test_cases:
        assert expected_type in result, f"Expected {expected_type} for {input_type}" 