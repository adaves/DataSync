# DataSync App Development - Planning & Checklist

## Project Overview
DataSync is a small but critical app for updating Microsoft Access databases using data from Microsoft Excel files. It will be used by 1-2 people. The app must be robust, user-friendly, and safe for business-critical data.

### Core Functionalities
1. **View table names in an Access database**
2. **Select a table and filter its data by date**
3. **Delete all data related to a specific date**
   - All deleted data is moved to a temporary table (named `table_name_5.1.2025_temp_table.accdb`) for about a week
4. **Upload new data from Excel to the selected Access table**

## Technical Requirements
- Python Version: 3.13.3
- Windows Only Application
- Command Line Interface with numbered menu (1-n)
- Large Data Handling: Up to 3 million rows
- Temp Table Retention: Default 7 days, configurable
- No security/login required
- Microsoft Office 2503 (Excel and Access)

## Development Approach
- **Test-Driven Development (TDD):**
  - Write unit tests first
  - Implement code to pass tests
  - Refactor as needed
- **Error Handling:**
  - Graceful handling of all errors
  - Clear error messages
  - Data integrity checks
- **Performance:**
  - Efficient handling of large datasets
  - Progress indicators for long operations

## Implementation Status

### ✅ Section 1: List Access Tables
- [x] Function: List all table names in a given Access database
  - [x] Implementation: `list_access_tables()` in `src/app/database/access_utils.py`
  - [x] Unit Tests: `tests/test_access_utils.py`
  - [x] Features:
    - Lists all user tables
    - Excludes system tables
    - Handles invalid/non-existent files
    - Uses context manager for connections
  - [x] Error Handling:
    - FileNotFoundError
    - AccessDatabaseError
    - Invalid file types

### 🔄 Section 2: Read/Filter Table Data

#### ✅ 2.1 Table Metadata Function
- [x] Function: `get_table_info(db_path: Path, table_name: str) -> TableInfo`
  - [x] Implementation:
    - Returns table structure (columns, types, keys)
    - Caches metadata for performance
    - Handles table not found
    - Handles connection errors
  - [x] Test Cases:
    - Valid table returns correct structure
    - Invalid table raises error
    - Cached data is used on subsequent calls
    - Cache invalidation on table changes
  - [x] CLI Integration:
    - Added "Show table structure" command
    - Displays column details in formatted table
    - Handles user input and navigation

#### ✅ 2.2 Date Input Handling
- [x] Function: `parse_date_input(date_str: str) -> DateFilter`
  - [x] Implementation:
    - Handles full date format (MM/DD/YYYY)
    - Handles year-only format (YYYY)
    - Generates appropriate SQL WHERE clause
    - Validates date formats
  - [x] Test Cases:
    - Full date input (e.g., "1/1/2025")
    - Year only input (e.g., "2025")
    - Invalid formats:
      - Wrong separators
      - Invalid months/days
      - Non-numeric input
      - Empty/None values
    - Edge Cases:
      - Leap years
      - Single digit months/days
      - Leading zeros
  - [x] SQL Generation:
    - Full date: exact match
    - Year only: range match (BETWEEN)
    - Handle Access SQL date format
    - Parameterized queries for safety

#### ✅ 2.3 Data Reading Function
- [x] Function: `read_filtered_data(db_path: Path, table_name: str, date_filter: DateFilter, chunk_size: int = 10000, progress_callback: Optional[Callable[[int, int], None]] = None) -> pd.DataFrame`
  - Implementation:
    - [x] Table metadata validation
    - [x] Date column detection
    - [x] SQL query construction with date filtering
    - [x] Chunked data reading
    - [x] Progress tracking
    - [x] Memory optimization
  - Test Cases:
    - [x] Read with full date filter
    - [x] Read with year-only filter
    - [x] Handle empty results
    - [x] Handle invalid table
    - [x] Test chunked reading
    - [x] Memory usage optimization
    - [x] Progress callback functionality
  - Error Handling:
    - [x] Invalid table
    - [x] Invalid date column
    - [x] Connection issues
    - [x] Memory management
  - Performance Features:
    - [x] Chunked reading for large datasets
    - [x] Progress tracking
    - [x] Memory-efficient DataFrame construction
    - [x] SQL-side filtering
    - [x] Metadata caching

#### Implementation Details:
1. **Table Metadata**:
   - Uses cached `get_table_info()` for performance
   - Validates table existence and structure
   - Detects date columns automatically

2. **Date Filtering**:
   - Uses `DateFilter` class for consistent date handling
   - Supports both full date and year-only filters
   - SQL-side filtering for performance

3. **Memory Management**:
   - Chunk-based reading (default 10k rows)
   - Efficient DataFrame construction
   - Memory usage monitoring
   - Cleanup of intermediate data

4. **Progress Tracking**:
   - Optional callback for progress updates
   - Accurate row counting
   - Real-time progress reporting

5. **Error Handling**:
   - Clear error messages
   - Proper resource cleanup
   - Type validation
   - Connection management

6. **Testing Infrastructure**:
   - Template database for consistent testing
   - Automated cleanup
   - Comprehensive test cases
   - Performance validation

#### Next Steps:
1. Move on to Section 3: Delete Data by Date
2. Consider additional optimizations:
   - Index usage for large tables
   - Parallel processing for huge datasets
   - Connection pooling
   - More granular progress reporting

### ✅ Section 3: Delete Data by Date
- [x] Function: Delete all data for a specific date (with backup to temp table)
  - [x] Implementation: `delete_data_by_date()` in `src/app/database/delete_operations.py`
  - [x] Unit Tests: `tests/test_delete_operations.py`
  - [x] Features:
    - Verifies table existence before operation
    - Creates a temporary table with same structure
    - Moves deleted data to temp table for safe keeping
    - Uses consistent naming format: `table_name_M.D.YYYY_temp_table`
    - Verifies data integrity with count check
  - [x] Error Handling:
    - Table not found
    - No data for specified date
    - Data integrity check failure
  - [x] Temp Table Management:
    - `cleanup_old_temp_tables()` to remove tables older than N days
    - Default 7-day retention
    - Configurable retention period

### ⏳ Section 4: Upload Excel Data
- [x] Function: Basic Excel file reading (Phase 1)
  - [x] Implementation: `excel_utils.py` in `src/app/processing/`
  - [x] Features:
    - Reading Excel files with progress reporting
    - Support for multiple sheets
    - Type inference and conversion
    - Efficient memory management
    - Metadata extraction
  - [x] Unit Tests: `tests/test_excel_utils.py`
- [x] Function: Data validation (Phase 2)
  - [x] Implementation: `data_validator.py` in `src/app/processing/`
  - [x] Features:
    - Comprehensive data validation against table schema
    - Column presence validation
    - Data type validation
    - Primary key/duplicate detection
    - Constraint validation (nulls, string length)
    - Detailed error reporting
  - [x] Unit Tests: `tests/test_data_validation.py`
- [x] Function: Upload to Access (Phase 3)
  - [x] Implementation: `upload_operations.py` in `src/app/database/`
  - [x] Features:
    - Data type conversion between Excel and Access
    - Batch processing for large datasets
    - Transaction management
    - String truncation for columns with length limits
    - Progress reporting
    - Detailed error handling and reporting
  - [x] Unit Tests: `tests/test_upload_operations.py`

### ✅ Section 5: CLI Interface
- [x] Main menu and navigation
  - [x] Numbered options (1-6)
  - [x] Clear formatting and organization
- [x] Database Selection
  - [x] Hybrid approach for selecting database:
    - Command-line argument
    - Current directory scanning
    - Recent databases history
    - Manual path entry
  - [x] Configuration storage for recent databases
- [x] Feature: Show Table Structure
  - [x] Display column names, data types, and properties
  - [x] Formatting for readability
- [x] Progress indicators for long operations
  - [x] Spinner animation for indeterminate operations
  - [x] Progress bars for operations with known size
- [x] User input handling and validation
- [x] Error messages with clear formatting
- [x] Configuration management

---

## Clarifying Questions & Answers
- **Access/Excel Version:** 2503
- **OS:** Windows only
- **Interface:** Simple command-line interface (CLI) with a menu. Menu appears when running `datasync`, options are numbered, and navigation (back/forth) is supported.
- **Data Size:** Largest Access DB ~3 million rows; Excel files can also be large.
- **Users:** Single user for now; may share with boss in future. No security/login needed.
- **Temp Table Retention:** Automatic cleanup, default 7 days, but user-configurable.
- **Usage Frequency:** Ad hoc
- **Virtual Environment:** Already set up and active
- **Python Version:** 3.13.3

---

## High-Level Workflow & Checklist

### 1. **Environment & Setup**
- [x] Confirm Python version and install required packages (pyodbc, pandas, openpyxl, etc.)
- [x] Set up a virtual environment
- [x] Create initial project structure

### 2. **Access Database Utilities**
- [x] Function: List all table names in a given Access database
- [x] Function: Read/filter data from a selected table by date
- [x] Function: Delete all data for a specific date (with backup to temp table)
- [ ] Function: Create/manage temp tables for deleted data
- [ ] Function: Clean up temp tables older than 1 week (default, but user-configurable)

### 3. **Excel Integration**
- [x] Function: Read data from Excel file (with type validation)
  - [x] Implementation: `read_excel_file()` in `src/app/processing/excel_utils.py`
  - [x] Unit Tests: `tests/test_excel_utils.py`
  - [x] Features:
    - Handles large Excel files
    - Validates data types
    - Supports multiple sheets
- [x] Function: Upload/append data to Access table (with error handling)
  - [x] Implementation: `upload_to_access()` in `src/app/database/table_operations.py`
  - [x] Unit Tests: `tests/test_table_operations.py`
  - [x] Features:
    - Batch processing
    - Transaction management
    - Error handling and rollback
- [x] Function: Validate data before upload (schema, types, duplicates)
  - [x] Implementation: `validate_data_for_table()` in `src/app/processing/data_validator.py`
  - [x] Unit Tests: `tests/test_data_validation.py`
  - [x] Features:
    - Schema validation
    - Data type checking
    - Duplicate detection
    - Constraint validation

### 4. **User Interface**
- [x] CLI menu system: numbered options, navigation, and clear prompts
- [ ] Implement basic user prompts/menus
- [ ] Add logging and error reporting

### 5. **Testing & Quality**
- [x] Write unit tests for each function (TDD)
  - [x] `tests/test_access_utils.py` for testing table listing
  - [x] `tests/test_table_metadata.py` for testing table info retrieval
  - [x] `tests/test_delete_operations.py` for testing delete operations
- [ ] Write integration tests for workflows
- [x] Document all functions and workflows

### 6. **Deployment & Usage**
- [ ] Create a simple README with usage instructions
- [ ] Package for easy installation/execution

---

## Best Practices & Pitfalls
- Always back up data before deletion or bulk updates
- Use parameterized queries to avoid SQL injection and type errors
- Handle file locking and concurrent access gracefully
- Validate all data types and handle missing/null values
- Log all operations and errors for traceability
- Clean up temp files/tables regularly

---

## Next Steps
1. **Section 4 is now complete!**
2. **Update the CLI interface to use the new upload functionality**
   - Add menu option for Excel data upload
   - Create user workflow for selecting Excel files
   - Add progress reporting
   - Display validation results to user
3. **Implement Enhanced Data Value Handling (Formatted Numeric Values)**
   - Handle Excel formatted values like currency ($100.50), percentages (25%), negative values (-$50)
   - Preserve original data format during processing
   - Convert values appropriately for Access database storage
   - TDD Implementation Plan:
     - Create ValueConverter for parsing numeric values from formatted strings
     - Implement DataFieldMapper to identify and map fields needing conversion
     - Update database operations to handle formatted values in SQL
     - Add comprehensive test suite for value conversion
   - Features: 
     - Automatic detection of formatted numeric fields
     - Correct conversion for database storage without altering original values
     - Roundtrip preservation (original → db format → original)
     - Detailed logging and diagnostics
4. Continue updating this scratchpad as a living checklist
5. Consider additional enhancements:
   - Better error logging
   - Configuration options for default behaviors
   - More detailed progress reporting

---

_This scratchpad is a living document. All changes, decisions, and progress will be tracked here as a checklist and reference._
