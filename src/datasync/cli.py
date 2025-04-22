"""
Command-line interface for the DataSync tool.
"""

import click
import os
from pathlib import Path
import pandas as pd
from typing import Optional
from datasync.database.operations import DatabaseOperations
from datasync.utils.logging import setup_logging
import logging
import time

logger = setup_logging()

def get_destination_type(destination: str) -> str:
    """Determine the destination type based on file extension."""
    if destination.lower().endswith('.accdb'):
        return 'access'
    elif destination.lower().endswith('.xlsx'):
        return 'excel'
    else:
        raise click.UsageError(f"Unsupported destination type: {destination}")

@click.group()
@click.version_option(version='1.0.0')
def cli():
    """DataSync CLI - Synchronize data between Access and Excel."""
    pass

@cli.command()
@click.argument('source', type=click.Path(exists=True))
@click.argument('destination', type=click.Path())
@click.option('--batch-size', type=int, default=1000, help='Number of records to process at once')
@click.option('--validate/--no-validate', default=True, help='Validate data after synchronization')
def sync(source, destination, batch_size, validate):
    """Synchronize data from source to destination."""
    try:
        if batch_size <= 0:
            raise click.UsageError("Batch size must be greater than 0")
        
        if not os.path.exists(source):
            raise click.UsageError(f"Source file does not exist: {source}")
        
        # Create destination directory if it doesn't exist
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        
        db_ops = DatabaseOperations()
        dest_type = get_destination_type(destination)
        
        if dest_type == 'access':
            db_ops.sync_to_access(source, destination, batch_size, validate)
        else:
            db_ops.sync_to_excel(source, destination, batch_size, validate)
            
        click.echo("Successfully synchronized data")
    except click.UsageError as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()

@cli.command()
@click.argument('database', type=click.Path(exists=True))
@click.option('--table', help='Specific table to validate')
@click.option('--year', type=int, help='Year to validate data for')
def validate(database, table, year):
    """Validate database structure and data."""
    try:
        if not os.path.exists(database):
            raise click.UsageError(f"Database file does not exist: {database}")
            
        db_ops = DatabaseOperations()
        db_ops.validate_database(database, table, year)
        click.echo("Database is valid")
    except click.UsageError as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()

@cli.command()
@click.argument('database', type=click.Path(exists=True))
@click.option('--interval', type=int, default=60, help='Monitoring interval in seconds')
@click.option('--duration', type=int, help='Total monitoring duration in seconds')
def monitor(database, interval, duration):
    """Monitor database operations."""
    try:
        if not os.path.exists(database):
            raise click.UsageError(f"Database file does not exist: {database}")
            
        if interval <= 0:
            raise click.UsageError("Interval must be greater than 0")
            
        db_ops = DatabaseOperations()
        db_ops.monitor_database(database, interval, duration)
    except click.UsageError as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()

if __name__ == '__main__':
    cli() 