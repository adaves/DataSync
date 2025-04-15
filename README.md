# DataSync

A Python-based data synchronization tool for Microsoft Access databases with robust validation and monitoring capabilities.

## Overview

DataSync provides a comprehensive solution for managing and synchronizing data in Microsoft Access databases. It includes:

- Database operations (CRUD)
- Data validation
- Performance monitoring
- Error tracking
- Logging capabilities

## Features

### Database Operations
- Connection management
- CRUD operations
- Transaction support
- Batch operations
- Table management

### Validation
- Data type validation
- Required field validation
- String length validation
- Date range validation
- Pattern matching
- Foreign key validation

### Monitoring
- Operation tracking
- Performance metrics
- Error tracking
- Detailed logging
- Performance reporting

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/DataSync.git
cd DataSync
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Project Structure

```
DataSync/
├── src/
│   ├── database/
│   │   ├── operations.py    # Core database operations
│   │   ├── validation.py    # Data validation
│   │   └── monitoring.py    # Performance monitoring
│   ├── processing/          # Data processing modules
│   ├── utils/              # Utility functions
│   └── tests/              # Test suite
├── docs/                   # Documentation
├── config/                 # Configuration files
├── logs/                   # Log files
└── requirements/           # Dependency management
```

## Usage

### Basic Database Operations

```python
from src.database.operations import DatabaseOperations

# Initialize database connection
db = DatabaseOperations("path/to/database.accdb")
db.connect()

try:
    # Perform operations
    tables = db.get_tables()
    data = db.read_table("TableName")
    
    # Execute custom query
    results = db.execute_query("SELECT * FROM TableName WHERE condition")
finally:
    db.close()
```

### Data Validation

```python
from src.database.validation import DatabaseValidation

validator = DatabaseValidation()

# Define validation rules
validation_rules = {
    'data_types': {
        'name': str,
        'age': int,
        'date': datetime
    },
    'required_fields': ['name', 'age'],
    'string_lengths': {'name': 50},
    'date_ranges': {'date': (min_date, max_date)}
}

# Validate data
errors = validator.validate_all(data, validation_rules)
```

### Performance Monitoring

```python
from src.database.monitoring import DatabaseMonitor

monitor = DatabaseMonitor()

# Track an operation
metrics = monitor.start_operation("SELECT", "SELECT * FROM TableName")
try:
    # Perform operation
    result = db.execute_query(metrics.query)
    monitor.end_operation(metrics, success=True, affected_rows=len(result))
except Exception as e:
    monitor.end_operation(metrics, success=False, error_message=str(e))

# Get performance report
report = monitor.get_performance_report()
```

## Development

### Running Tests

```bash
python -m pytest src/tests
```

### Code Style

This project follows PEP 8 style guidelines. Use the following tools to maintain code quality:

```bash
# Install development dependencies
pip install -r requirements/dev.txt

# Run linting
flake8 src/

# Run type checking
mypy src/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Microsoft Access Database Engine
- Python community
- Open source contributors 