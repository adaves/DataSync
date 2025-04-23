# DataSync

A Python-based data synchronization tool for Microsoft Access databases with robust validation and monitoring capabilities.

## Prerequisites

- Python 3.8 or higher
- Microsoft Access Database Engine (for Windows)
- pip (Python package installer)

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

### Option 1: Global Installation (Recommended for Production Use)

1. Clone the repository:
```bash
git clone https://github.com/yourusername/DataSync.git
cd DataSync
```

2. Install globally:
```bash
pip install -e .
```

This will make the `datasync` command available system-wide, and you can run it from anywhere without activating a virtual environment.

### Option 2: Development Installation (For Contributors)

1. Clone the repository:
```bash
git clone https://github.com/yourusername/DataSync.git
cd DataSync
```

2. Create and activate a virtual environment:
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/MacOS
python -m venv venv
source venv/bin/activate
```

3. Install the package in development mode:
```bash
pip install -e .
```

4. Install development dependencies:
```bash
pip install -e ".[dev]"
```

## Running the Application

### Basic Usage

Run the application using the command-line interface:
```bash
datasync [options]
```

### Development Commands

1. Run tests:
```bash
pytest
```

2. Run tests with coverage:
```bash
pytest --cov=src/datasync --cov-report=term-missing
```

3. Generate HTML coverage report:
```bash
pytest --cov=src/datasync --cov-report=html
```

4. Run type checking:
```bash
mypy src
```

5. Run linting:
```bash
ruff check src
```

6. Format code:
```bash
black src
isort src
```

### Configuration

1. Create a `.env` file in the root directory with your database configuration:
```
DB_DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}
DB_PATH=path/to/your/database.accdb
```

2. Update the configuration in `config/` directory as needed.

## Project Structure

```
DataSync/
├── src/
│   └── datasync/
│       ├── cli.py
│       ├── database/
│       ├── processing/
│       └── utils/
├── tests/
├── config/
├── docs/
└── logs/
```

## Usage

### Basic Database Operations

```