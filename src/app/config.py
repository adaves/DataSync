"""
Configuration management for DataSync application.
Handles storing and retrieving user preferences and database history.
"""

import os
import json
from pathlib import Path
from typing import List, Optional

# Constants
CONFIG_DIR = Path.home() / ".datasync"
CONFIG_FILE = CONFIG_DIR / "config.json"
MAX_HISTORY_ITEMS = 10

# Default configuration
DEFAULT_CONFIG = {
    "recent_databases": [],
    "temp_retention_days": 7
}

def ensure_config_exists():
    """Ensure configuration directory and file exist."""
    # Create config directory if it doesn't exist
    if not CONFIG_DIR.exists():
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create config file with defaults if it doesn't exist
    if not CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'w') as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)

def get_config():
    """Get current configuration."""
    ensure_config_exists()
    
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        # If config is corrupted or missing, reset to defaults
        return DEFAULT_CONFIG.copy()

def save_config(config):
    """Save configuration to file."""
    ensure_config_exists()
    
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)

def add_database_to_history(db_path: Path):
    """
    Add database to recent history.
    
    Args:
        db_path: Path to the database
    """
    db_path_str = str(db_path.absolute())
    
    config = get_config()
    recent_dbs = config.get("recent_databases", [])
    
    # Remove if already exists (to move to top)
    if db_path_str in recent_dbs:
        recent_dbs.remove(db_path_str)
    
    # Add to start of list
    recent_dbs.insert(0, db_path_str)
    
    # Trim list if too long
    config["recent_databases"] = recent_dbs[:MAX_HISTORY_ITEMS]
    
    save_config(config)

def get_recent_databases() -> List[str]:
    """
    Get list of recent databases.
    
    Returns:
        List of database paths
    """
    config = get_config()
    return config.get("recent_databases", [])

def find_access_databases_in_directory(directory: Path = None) -> List[Path]:
    """
    Find Access databases in the specified directory.
    
    Args:
        directory: Directory to search (defaults to current directory)
        
    Returns:
        List of database paths
    """
    if directory is None:
        directory = Path.cwd()
    
    # Look for .accdb and .mdb files
    databases = []
    for ext in [".accdb", ".mdb"]:
        databases.extend(directory.glob(f"*{ext}"))
    
    return sorted(databases)

def get_default_database() -> Optional[Path]:
    """
    Get default database using multiple strategies:
    1. Check recent databases and return first valid one
    2. Check current directory for Access databases
    
    Returns:
        Path to default database or None if not found
    """
    # Check recent databases
    for db_path_str in get_recent_databases():
        db_path = Path(db_path_str)
        if db_path.exists():
            return db_path
    
    # Check current directory
    databases = find_access_databases_in_directory()
    if databases:
        return databases[0]
    
    return None 