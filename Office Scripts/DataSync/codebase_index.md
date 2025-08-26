# DataSync - Codebase Index & Description

## Project Overview

**DataSync** is a Python-based data synchronization and management tool specifically designed for Microsoft Access databases. The project provides a comprehensive solution for viewing, editing, importing, and managing data in Access databases with robust validation, error handling, and recovery capabilities.

### Key Features

- **Database Operations**: Full CRUD (Create, Read, Update, Delete) operations for Microsoft Access databases
- **Excel Integration**: Import/export data between Excel workbooks and Access databases with intelligent column mapping
- **Interactive CLI**: User-friendly command-line interface with menu-driven operations
- **Data Validation**: Comprehensive validation for data types, required fields, string lengths, date ranges, and foreign keys
- **Transaction Support**: Safe database operations with transaction management and rollback capabilities
- **Record Recovery**: Advanced deletion tracking with ability to recover accidentally deleted records
- **Batch Processing**: Efficient handling of large datasets with configurable batch sizes
- **Real-time Progress Tracking**: Visual progress indicators for long-running operations
- **Comprehensive Testing**: Full test suite with unit and integration tests
- **Logging & Monitoring**: Detailed logging with configurable levels and file outputs

### Architecture

The project follows modern Python development practices with:
- **Modular Design**: Clean separation of concerns across database, processing, and utility modules
- **Type Hints**: Full type annotation for better code maintainability
- **Error Handling**: Robust exception handling with custom error types
- **Configuration Management**: YAML-based configuration with environment-specific settings
- **Comprehensive Testing**: pytest-based testing with fixtures and mock databases

### Target Use Cases

- Office environments requiring Access database management
- Data migration between Excel and Access systems
- Database maintenance and recovery operations
- Automated data processing workflows
- Educational and training environments for database operations

---

## File Structure

This section provides a detailed tree view of the file structure in the DataSync project.

```
DataSync/
├── build/
│   └── DataSync/
│       ├── localpycs/ (empty)
│       ├── DataSync.pkg
│       ├── PKG-00.toc
│       ├── PYZ-00.pyz
│       ├── PYZ-00.toc
│       ├── Analysis-00.toc
│       ├── warn-DataSync.txt
│       ├── xref-DataSync.html
│       └── base_library.zip
├── config/
│   ├── logging.yaml
│   └── settings.yaml
├── dist/
│   └── DataSync.exe
├── docs/
│   ├── 4.10.2025 - All Other - Projected KPI's COT - TG Fixed Public - Default.xls
│   ├── database_processing_documentation.txt
│   ├── Database11.accdb
│   ├── ideas.txt
│   └── project_structure_checklist.txt
├── examples/ (empty)
├── htmlcov/
│   ├── class_index.html
│   ├── coverage_html_cb_497bf287.js
│   ├── favicon_32_cb_58284776.png
│   ├── function_index.html
│   ├── index.html
│   ├── keybd_closed_cb_ce680311.png
│   ├── status.json
│   ├── style_cb_718ce007.css
│   ├── z_895837f81467de1f_mock_database_py.html
│   ├── z_895837f81467de1f_error_handling_py.html
│   ├── z_895837f81467de1f_path_utils_py.html
│   ├── z_e4e394f66fe6788e_operations_py.html
│   ├── z_e4e394f66fe6788e_transaction_py.html
│   ├── z_895837f81467de1f_helpers_py.html
│   ├── z_895837f81467de1f_logging_py.html
│   ├── z_dfaeb46fc30a5447_cli_py.html
│   ├── z_e4e394f66fe6788e_sql_syntax_py.html
│   ├── z_e4e394f66fe6788e_validation_py.html
│   ├── .gitignore
│   ├── z_895837f81467de1f_config_py.html
│   ├── z_895837f81467de1f_logger_py.html
│   ├── z_c4febb437a431b3d_excel_processor_py.html
│   ├── z_c4febb437a431b3d_file_manager_py.html
│   ├── z_c4febb437a431b3d_validation_py.html
│   ├── z_e4e394f66fe6788e_exceptions_py.html
│   └── z_e4e394f66fe6788e_monitoring_py.html
├── logs/
│   ├── __main__.log
│   ├── datasync.cli.log
│   ├── db_operations_20250410_151206.log
│   ├── db_operations_20250410_151800.log
│   ├── db_operations_20250410_152100.log
│   ├── db_operations_20250410_153203.log
│   ├── db_operations_20250410_153643.log
│   ├── db_operations_20250410_154529.log
│   ├── db_operations_20250410_155338.log
│   ├── db_operations_20250410_161216.log
│   ├── db_operations_20250410_162113.log
│   ├── db_operations_20250410_163325.log
│   ├── db_operations_20250410_163356.log
│   ├── db_operations_20250410_164649.log
│   ├── db_operations_20250410_165047.log
│   ├── db_operations_20250410_165158.log
│   ├── db_operations_20250410_165935.log
│   ├── db_operations_20250411_102248.log
│   ├── db_operations_20250411_102746.log
│   ├── db_operations_20250411_103418.log
│   └── db_operations_20250411_110108.log
├── requirements/ (empty)
├── src/
│   ├── .coverage
│   ├── database/
│   │   └── operations.py
│   ├── datasync/
│   │   ├── __init__.py
│   │   ├── cli.py
│   │   ├── database/
│   │   │   └── __init__.py
│   │   ├── processing/
│   │   │   ├── __init__.py
│   │   │   ├── excel_processor.py
│   │   │   ├── file_manager.py
│   │   │   └── validation.py
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── config.py
│   │       ├── error_handling.py
│   │       ├── helpers.py
│   │       ├── logger.py
│   │       ├── logging.py
│   │       ├── mock_database.py
│   │       ├── path_utils.py
│   │       └── progress.py
│   ├── datasync.egg-info/
│   │   ├── dependency_links.txt
│   │   ├── entry_points.txt
│   │   ├── PKG-INFO
│   │   ├── requires.txt
│   │   ├── SOURCES.txt
│   │   └── top_level.txt
│   ├── logs/
│   │   └── datasync.cli.log
│   ├── processing/ (empty)
│   └── tests/
│       ├── conftest.py
│       ├── fixtures/
│       │   ├── database.py
│       │   ├── logs/ (empty)
│       │   ├── mock_database/
│       │   │   ├── create_mock_db.py
│       │   │   ├── mock_database.accdb
│       │   │   └── README.md
│       │   ├── output/ (empty)
│       │   ├── sample_data/
│       │   │   └── README.md
│       │   ├── temp/ (empty)
│       │   └── test_configs/ (empty)
│       ├── integration/
│       │   ├── test_database_integration.py
│       │   └── test_file_processing_integration.py
│       └── unit/
│           ├── test_cli.py
│           ├── database/
│           │   ├── test_operations.py
│           │   ├── test_sql_syntax.py
│           │   ├── test_validation.py
│           │   └── .coverage
│           ├── processing/
│           │   ├── test_excel_processor.py
│           │   ├── test_file_manager.py
│           │   └── test_validation.py
│           └── utils/
│               ├── test_config.py
│               ├── test_error_handling.py
│               ├── test_helpers.py
│               ├── test_logging.py
│               ├── test_path_utils.py
│               └── test_progress.py
├── .cursor/
│   └── rules/ (empty)
├── build.py
├── DataSync_Implementation_Quiz.md
├── DataSync_Quiz_Answer_Key.md
├── DataSync.spec
├── pyproject.toml
├── pytest.ini
├── run_datasync.bat
├── setup.py
└── User_Instructions.md
```

Note: __pycache__ directories are omitted from this tree as they are generated and not part of the source code. Similarly, some build and cache files may not be listed explicitly if not relevant for source indexing.

---

## Module Breakdown

### Core Application (`src/datasync/`)

#### CLI Module (`cli.py`)
- **Interactive Menu System**: Multi-level menu interface for database operations
- **Command-line Interface**: Direct commands for sync, validate, and menu operations  
- **Database Auto-discovery**: Automatically finds databases in common locations
- **Table Operations**: View, add, delete, and recover records with real-time feedback
- **Excel Import/Export**: Column mapping and batch processing capabilities
- **Progress Tracking**: Real-time progress updates for long-running operations

#### Database Module (`database/`)
- **Connection Management**: Robust pyodbc-based connections with error handling
- **CRUD Operations**: Complete Create, Read, Update, Delete functionality
- **Transaction Support**: Safe operations with commit/rollback capabilities  
- **Record Recovery**: Tracks deletions and enables recovery of deleted records
- **SQL Query Execution**: Parameterized query support with result handling
- **Table Management**: Dynamic table creation and schema management

#### Processing Module (`processing/`)
- **Excel Processor**: Read/write Excel files with multi-sheet support
- **File Manager**: File system operations and path management
- **Data Validation**: Comprehensive validation for data types, formats, and constraints
- **Column Mapping**: Intelligent mapping between Excel and database columns
- **Batch Processing**: Efficient handling of large datasets

#### Utils Module (`utils/`)
- **Configuration Management**: YAML-based config loading and environment handling
- **Logging System**: Multi-level logging with file and console outputs
- **Error Handling**: Custom exception classes and error recovery
- **Path Utilities**: Cross-platform path normalization and validation
- **Progress Tracking**: Visual progress bars and percentage indicators
- **Helper Functions**: Common utilities for data processing and validation

### Testing Framework (`src/tests/`)

#### Unit Tests
- **Database Operations**: Comprehensive testing of all CRUD operations
- **Excel Processing**: Testing of file reading, writing, and validation
- **Utils Testing**: Coverage of configuration, logging, and utility functions
- **Validation Testing**: Data type and constraint validation testing

#### Integration Tests  
- **Database Integration**: Real database operations with Access files
- **File Processing Integration**: End-to-end Excel-to-database workflows
- **CLI Integration**: Testing of menu system and command-line operations

#### Test Fixtures
- **Mock Database**: Sample Access database with realistic schema and data
- **Sample Data**: Excel files with various data types and edge cases
- **Test Configurations**: Environment-specific test settings

### Configuration & Documentation

#### Configuration Files (`config/`)
- **logging.yaml**: Logging configuration with multiple handlers and formatters
- **settings.yaml**: Application settings and default values

#### Documentation (`docs/`)
- **Implementation Guide**: Detailed implementation documentation
- **User Instructions**: Step-by-step user guide with examples
- **Database Documentation**: Schema and processing workflow documentation
- **Project Checklist**: Development and deployment checklist

### Build & Distribution

#### Build System
- **PyInstaller Spec**: Configuration for creating standalone executable
- **Requirements**: Dependencies and development requirements
- **Setup Configuration**: Package metadata and entry points
- **Build Script**: Automated build process for executable creation

#### Development Tools
- **pytest Configuration**: Test discovery and coverage settings
- **Code Quality**: Ruff formatting, mypy type checking, coverage reporting
- **CI/CD Ready**: Configuration for automated testing and deployment

---

## Technology Stack

- **Python 3.8+**: Core language with modern features and type hints
- **pyodbc**: Microsoft Access database connectivity 
- **pandas**: Data manipulation and analysis
- **click**: Command-line interface framework
- **openpyxl**: Excel file processing
- **pytest**: Testing framework with fixtures and coverage
- **PyInstaller**: Executable creation for distribution
- **YAML**: Configuration file format
- **pathlib**: Modern path handling

---

## Development Workflow

The project supports a complete development workflow:

1. **Development**: Modular code structure with clear separation of concerns
2. **Testing**: Comprehensive test suite with both unit and integration tests
3. **Quality Assurance**: Code formatting, linting, and type checking
4. **Documentation**: Inline documentation and user guides
5. **Building**: Automated executable creation for distribution
6. **Deployment**: Standalone executable for end-user distribution

The codebase is well-structured for maintainability, with clear module boundaries, comprehensive error handling, and extensive testing coverage. 