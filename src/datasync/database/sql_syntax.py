"""
SQL syntax conversion utilities for Microsoft Access compatibility.
This module provides functions to convert standard SQL syntax to Microsoft Access specific syntax.
"""

from typing import Dict, Any, List, Optional

class AccessSQLSyntax:
    """Handles SQL syntax conversion for Microsoft Access compatibility."""
    
    # Mapping of standard SQL types to Access types
    TYPE_MAPPING = {
        'TEXT': 'VARCHAR',
        'INTEGER': 'INT',
        'DOUBLE': 'DOUBLE',
        'DECIMAL': 'CURRENCY',
        'DATETIME': 'DATETIME',
        'BOOLEAN': 'YESNO',
        'BLOB': 'LONGBINARY',
        'COUNTER': 'AUTOINCREMENT'
    }
    
    @staticmethod
    def convert_create_table(table_name: str, columns: Dict[str, str], 
                           primary_key: Optional[List[str]] = None) -> str:
        """
        Convert standard CREATE TABLE syntax to Access syntax.
        
        Args:
            table_name: Name of the table
            columns: Dictionary of column names and their types
            primary_key: Optional list of primary key columns
            
        Returns:
            Access-compatible CREATE TABLE statement
        """
        column_defs = []
        
        for col_name, col_type in columns.items():
            # Convert type to Access type
            access_type = AccessSQLSyntax.TYPE_MAPPING.get(col_type.upper(), col_type)
            
            # Handle special cases
            if access_type == 'VARCHAR':
                # Access requires length for VARCHAR
                access_type = 'VARCHAR(255)'
            elif access_type == 'CURRENCY':
                # Access CURRENCY type doesn't need precision/scale
                access_type = 'CURRENCY'
            
            column_defs.append(f"[{col_name}] {access_type}")
        
        # Add primary key if specified
        if primary_key:
            pk_cols = ', '.join(f"[{col}]" for col in primary_key)
            column_defs.append(f"PRIMARY KEY ({pk_cols})")
        
        return f"CREATE TABLE [{table_name}] (\n    {',\n    '.join(column_defs)}\n)"
    
    @staticmethod
    def convert_alter_table(table_name: str, 
                          add_columns: Optional[Dict[str, str]] = None,
                          drop_columns: Optional[List[str]] = None) -> str:
        """
        Convert standard ALTER TABLE syntax to Access syntax.
        
        Args:
            table_name: Name of the table
            add_columns: Dictionary of columns to add and their types
            drop_columns: List of columns to drop
            
        Returns:
            Access-compatible ALTER TABLE statement
        """
        statements = []
        
        if add_columns:
            for col_name, col_type in add_columns.items():
                access_type = AccessSQLSyntax.TYPE_MAPPING.get(col_type.upper(), col_type)
                if access_type == 'VARCHAR':
                    access_type = 'VARCHAR(255)'
                statements.append(
                    f"ALTER TABLE [{table_name}] ADD COLUMN [{col_name}] {access_type}"
                )
        
        if drop_columns:
            for col_name in drop_columns:
                statements.append(
                    f"ALTER TABLE [{table_name}] DROP COLUMN [{col_name}]"
                )
        
        return ';\n'.join(statements)
    
    @staticmethod
    def convert_limit_offset(query: str, limit: Optional[int] = None, 
                           offset: Optional[int] = None) -> str:
        """
        Convert LIMIT/OFFSET syntax to Access TOP syntax.
        
        Args:
            query: Original SQL query
            limit: Maximum number of records
            offset: Number of records to skip
            
        Returns:
            Access-compatible query with TOP clause
        """
        if limit is None and offset is None:
            return query
            
        # Access doesn't support OFFSET, so we need to use a different approach
        if offset is not None:
            raise NotImplementedError(
                "OFFSET is not supported in Microsoft Access. "
                "Consider using a different pagination approach."
            )
            
        # Convert LIMIT to TOP
        if limit is not None:
            # Find the SELECT keyword
            select_pos = query.upper().find('SELECT')
            if select_pos == -1:
                return query
                
            # Insert TOP after SELECT
            return query[:select_pos + 6] + f" TOP {limit}" + query[select_pos + 6:]
            
        return query
    
    @staticmethod
    def convert_foreign_key(table_name: str, column: str, 
                          ref_table: str, ref_column: str) -> str:
        """
        Convert standard FOREIGN KEY syntax to Access syntax.
        
        Args:
            table_name: Name of the table
            column: Name of the foreign key column
            ref_table: Name of the referenced table
            ref_column: Name of the referenced column
            
        Returns:
            Access-compatible FOREIGN KEY constraint
        """
        return (
            f"ALTER TABLE [{table_name}] "
            f"ADD CONSTRAINT FK_{table_name}_{column} "
            f"FOREIGN KEY ([{column}]) "
            f"REFERENCES [{ref_table}] ([{ref_column}])"
        )
    
    @staticmethod
    def convert_not_null(column_def: str) -> str:
        """
        Convert NOT NULL constraint to Access syntax.
        
        Args:
            column_def: Column definition string
            
        Returns:
            Access-compatible NOT NULL constraint
        """
        return column_def.replace('NOT NULL', 'NOT NULL') 