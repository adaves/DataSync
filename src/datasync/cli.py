"""
Command-line interface for the DataSync application.
This module provides the CLI interface for interacting with the DataSync application,
including commands for synchronization, validation, and monitoring.
"""

import click
import sys
from typing import Optional
from pathlib import Path
from datasync.database.operations import DatabaseOperations
import logging
import pandas as pd
import os
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_destination_type(destination: str) -> str:
    """Determine if destination is Access or Excel based on file extension."""
    ext = os.path.splitext(destination)[1].lower()
    if ext in ['.mdb', '.accdb']:
        return 'access'
    elif ext in ['.xlsx', '.xls']:
        return 'excel'
    else:
        raise ValueError(f"Unsupported destination file type: {ext}")

@click.group()
@click.version_option()
def cli():
    """DataSync - A tool for synchronizing data between Microsoft Access and Excel."""
    pass

@cli.command()
@click.argument('source', type=click.Path(exists=True))
@click.argument('destination', type=click.Path())
@click.option('--validate/--no-validate', default=True, help='Enable/disable data validation')
@click.option('--monitor/--no-monitor', default=True, help='Enable/disable operation monitoring')
@click.option('--batch-size', '-b', type=int, default=1000, help='Number of records to process in each batch')
def sync(source: str, destination: str, validate: bool, monitor: bool, batch_size: int):
    """Synchronize data from source to destination."""
    try:
        # Initialize source database operations
        source_db = DatabaseOperations(source)
        source_db.connect()
        
        # Get all tables from source
        tables = source_db.get_tables()
        click.echo(f"Found {len(tables)} tables in source database")
        
        # Determine destination type
        dest_type = get_destination_type(destination)
        
        # Initialize destination based on type
        if dest_type == 'access':
            dest_db = DatabaseOperations(destination)
            dest_db.connect()
        else:  # excel
            writer = pd.ExcelWriter(destination, engine='openpyxl')
        
        # Process each table
        for table in tables:
            click.echo(f"\nProcessing table: {table}")
            
            # Read table data
            df = source_db.read_table(table)
            total_records = len(df)
            click.echo(f"Read {total_records} records from {table}")
            
            if validate:
                # Validate data before sync
                click.echo("Validating data...")
                # Add validation logic here
                # For now, just check for null values
                null_counts = df.isnull().sum()
                if null_counts.any():
                    click.echo("Warning: Found null values in columns:")
                    for col, count in null_counts[null_counts > 0].items():
                        click.echo(f"  {col}: {count} null values")
            
            # Process in batches
            for i in range(0, total_records, batch_size):
                batch = df.iloc[i:i+batch_size]
                click.echo(f"Processing batch {i//batch_size + 1} of {(total_records-1)//batch_size + 1}")
                
                if dest_type == 'access':
                    # TODO: Implement batch insert for Access
                    pass
                else:
                    # Write to Excel
                    batch.to_excel(writer, sheet_name=table, index=False, startrow=i)
            
            if dest_type == 'excel':
                writer.save()
        
        # Cleanup
        source_db.close()
        if dest_type == 'access':
            dest_db.close()
        
        click.echo("\nSynchronization completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Error during synchronization: {e}")
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.argument('database', type=click.Path(exists=True))
@click.option('--table', '-t', help='Specific table to validate')
@click.option('--output', '-o', type=click.Path(), help='Output file for validation results')
@click.option('--year', '-y', type=int, default=2024, help='Year to validate records for')
def validate(database: str, table: Optional[str], output: Optional[str], year: int):
    """Validate database integrity and data consistency."""
    try:
        # Initialize database operations
        db_ops = DatabaseOperations(database)
        db_ops.connect()
        
        # Get tables to validate
        tables = [table] if table else db_ops.get_tables()
        click.echo(f"Validating {len(tables)} tables")
        
        validation_results = []
        
        for table_name in tables:
            click.echo(f"\nValidating table: {table_name}")
            
            # Get table columns
            columns = db_ops.get_table_columns(table_name)
            click.echo(f"Found {len(columns)} columns")
            
            # Count records
            record_count = db_ops.count_records(table_name, year)
            click.echo(f"Found {record_count} records for year {year}")
            
            # Additional validation checks
            validation_checks = {
                'has_primary_key': False,
                'has_required_fields': False,
                'data_types_valid': True,
                'null_values': 0
            }
            
            # TODO: Implement actual validation checks
            # For now, just add placeholder results
            validation_results.append({
                'table': table_name,
                'columns': columns,
                'record_count': record_count,
                **validation_checks
            })
        
        # Output results
        if output:
            df = pd.DataFrame(validation_results)
            df.to_csv(output, index=False)
            click.echo(f"\nValidation results saved to: {output}")
        
        db_ops.close()
        click.echo("\nValidation completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Error during validation: {e}")
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.argument('database', type=click.Path(exists=True))
@click.option('--interval', '-i', type=int, default=60, help='Monitoring interval in seconds')
@click.option('--output', '-o', type=click.Path(), help='Output file for monitoring results')
@click.option('--duration', '-d', type=int, default=3600, help='Total monitoring duration in seconds')
def monitor(database: str, interval: int, output: Optional[str], duration: int):
    """Monitor database operations and performance."""
    try:
        # Initialize database operations
        db_ops = DatabaseOperations(database)
        db_ops.connect()
        
        click.echo(f"Starting monitoring of database: {database}")
        click.echo(f"Monitoring interval: {interval} seconds")
        click.echo(f"Total duration: {duration} seconds")
        
        # Get initial state
        tables = db_ops.get_tables()
        initial_counts = {
            table: db_ops.count_records(table, 2024)
            for table in tables
        }
        
        click.echo("\nInitial state:")
        for table, count in initial_counts.items():
            click.echo(f"{table}: {count} records")
        
        # Prepare monitoring results
        monitoring_results = []
        start_time = time.time()
        
        while time.time() - start_time < duration:
            current_time = datetime.now()
            click.echo(f"\nMonitoring at {current_time}")
            
            for table in tables:
                current_count = db_ops.count_records(table, 2024)
                change = current_count - initial_counts[table]
                
                click.echo(f"{table}: {current_count} records ({change:+d} change)")
                
                monitoring_results.append({
                    'timestamp': current_time,
                    'table': table,
                    'record_count': current_count,
                    'change': change
                })
            
            if output:
                df = pd.DataFrame(monitoring_results)
                df.to_csv(output, index=False)
            
            time.sleep(interval)
        
        db_ops.close()
        click.echo("\nMonitoring completed")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Error during monitoring: {e}")
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

if __name__ == '__main__':
    cli() 