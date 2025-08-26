# Sample Data Fixtures

This directory contains sample Excel files used for testing the Excel processing functionality.

## File Descriptions

1. `simple_data.xlsx`
   - Single sheet with basic data
   - Columns: id, name, value
   - Used for basic functionality testing

2. `complex_data.xlsx`
   - Multiple sheets with different structures
   - Sheet1: id, name, value
   - Sheet2: id, description, amount
   - Used for advanced functionality testing

3. `validation_data.xlsx`
   - Various data types and edge cases
   - Used for validation testing

## Usage

These files are used in both unit tests and integration tests to verify the Excel processing functionality. The files are automatically copied to a temporary directory during test execution.

## Adding New Test Data

When adding new test data:
1. Create the Excel file with appropriate test data
2. Update this README with the file description
3. Add corresponding test cases in the test files 