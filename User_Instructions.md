# DataSync - User Instructions

## Quick Start Guide

### 1. Installation

#### Option 1: Using the Executable (Recommended)
1. Download the DataSync.exe file
2. Open Command Prompt (cmd.exe)
3. Navigate to the folder containing DataSync.exe
4. Run the application by typing: `DataSync.exe`

#### Option 2: Using the Installer
1. Download the DataSync installer
2. Double-click the installer file
3. Follow the installation wizard instructions
4. Click "Finish" when installation is complete

### 2. First-Time Setup

1. Open Command Prompt (cmd.exe)
2. Navigate to the folder containing DataSync
3. Run the application: `DataSync.exe`
4. When prompted, enter the path to your Microsoft Access database file (.accdb or .mdb)
5. Press Enter to establish the connection

### 3. Command-Line Arguments

You can start the application with various command-line arguments:

```bash
# Basic usage
DataSync.exe

# Connect to a specific database
DataSync.exe --db "C:\path\to\database.accdb"

# Enable debug mode
DataSync.exe --debug

# Set log level
DataSync.exe --log-level INFO

# Show version
DataSync.exe --version

# Show help
DataSync.exe --help
```

### 4. Basic Operations

#### Viewing Data
```bash
# View all records in a table
view customers

# View with pagination
view customers --page 1 --limit 10

# View specific columns
view customers --columns id,name,email

# View with sorting
view customers --sort-by name --sort-order desc
```

#### Adding New Records
```bash
# Add a single record
add customers
# Then follow the prompts to enter values

# Add multiple records from a file
add customers --file new_customers.csv

# Add with specific values
add customers --values "name=John Doe,email=john@example.com,phone=1234567890"
```

#### Editing Records
```bash
# Edit a single record
edit customers 123
# Then follow the prompts to update values

# Edit multiple records
edit customers --where "status='inactive'" --set "status='active'"

# Edit with specific values
edit customers 123 --values "name=John Smith,email=john.smith@example.com"
```

#### Deleting Records
```bash
# Delete a single record
delete customers 123

# Delete multiple records
delete customers --where "status='inactive'"

# Delete with confirmation
delete customers 123 --confirm
```

### 5. Common Tasks

#### Searching for Data
```bash
# Basic search
search customers "John"

# Search in specific field
search customers "email=john@example.com"

# Advanced search with multiple conditions
search customers "name=John AND status=active"

# Search with wildcards
search customers "name=J*"
```

#### Exporting Data
```bash
# Export to Excel
export customers excel customers_data.xlsx

# Export to CSV
export customers csv customers_data.csv

# Export with specific columns
export customers excel customers_data.xlsx --columns id,name,email

# Export with filters
export customers excel customers_data.xlsx --where "status='active'"
```

#### Importing Data
```bash
# Import from Excel
import customers customers_data.xlsx

# Import from CSV
import customers customers_data.csv

# Import with column mapping
import customers customers_data.xlsx --map "First Name=name,Email Address=email"

# Import with update mode
import customers customers_data.xlsx --mode update
```

### 6. Advanced Operations

#### Batch Operations
```bash
# Batch update
batch update customers --file updates.csv

# Batch delete
batch delete customers --file ids_to_delete.txt
```

#### Data Validation
```bash
# Validate table data
validate customers

# Validate specific records
validate customers --ids 123,456,789

# Validate with custom rules
validate customers --rules validation_rules.json
```

#### Backup and Restore
```bash
# Create backup
backup create backup_20240321.accdb

# Restore from backup
backup restore backup_20240321.accdb

# List backups
backup list
```

### 7. Command Reference

#### Basic Commands
- `view [table] [options]` - View table contents
- `add [table] [options]` - Add new record
- `edit [table] [id] [options]` - Edit existing record
- `delete [table] [id] [options]` - Delete record
- `search [table] [term] [options]` - Search records
- `export [table] [format] [file] [options]` - Export data
- `import [table] [file] [options]` - Import data

#### Utility Commands
- `help` - Show available commands
- `help [command]` - Show detailed help for a command
- `exit` - Exit the application
- `clear` - Clear the screen
- `history` - Show command history

#### System Commands
- `status` - Show system status
- `config` - Show/update configuration
- `log` - View application logs
- `version` - Show application version

### 8. Command Options

Most commands support the following options:
- `--help` - Show command help
- `--verbose` - Show detailed output
- `--quiet` - Show minimal output
- `--format [format]` - Specify output format (table, json, csv)
- `--output [file]` - Save output to file
- `--where [condition]` - Filter records
- `--limit [number]` - Limit number of results
- `--sort-by [column]` - Sort results
- `--sort-order [asc|desc]` - Sort order

### 9. Examples

#### Complex Search Example
```bash
search customers "name=John* AND (status=active OR status=pending) AND created_date>'2024-01-01'" --limit 10 --sort-by created_date --sort-order desc
```

#### Advanced Export Example
```bash
export customers excel customers_report.xlsx --columns id,name,email,status --where "status='active'" --sort-by name --format table
```

#### Batch Update Example
```bash
batch update customers --file updates.csv --mode upsert --validate --log-file update_log.txt
```

### 10. Troubleshooting

#### Common Issues

1. **Can't Connect to Database**
   - Make sure the database file exists
   - Check that you have permission to access the file
   - Verify Microsoft Access is installed

2. **Error Saving Changes**
   - Check that all required fields are filled
   - Verify data types match the requirements
   - Make sure you have write permissions

3. **Slow Performance**
   - Try closing other applications
   - Consider breaking large operations into smaller batches
   - Check your computer's available memory

#### Getting Help

1. Type `help` to see available commands
2. Type `help [command]` to see detailed help for a specific command
3. Contact support at support@datasync.com

### 11. Best Practices

1. **Regular Backups**
   - Always keep a backup of your database
   - Create backups before making major changes
   - Store backups in a safe location

2. **Data Entry**
   - Double-check entries before saving
   - Use consistent formatting
   - Fill in all required fields

3. **Security**
   - Don't share your login credentials
   - Keep your computer secure

### 12. Contact Information

For additional help:
- Email: support@datasync.com
- Phone: (555) 123-4567
- Support Hours: Monday-Friday, 9AM-5PM EST 

### 13. Bulk Year-Based Operations

#### Deleting Records by Year
For large-scale deletion of records from a specific year (e.g., 2025):

```bash
# Delete all records from year 2025
delete records --where "YEAR(Time) = 2025" --batch-size 10000

# With progress tracking
delete records --where "YEAR(Time) = 2025" --batch-size 10000 --progress

# With backup before deletion
delete records --where "YEAR(Time) = 2025" --backup --backup-file backup_2025.accdb
```

#### Adding Large Numbers of Records
To add a large number of records (80,000-100,000):

```bash
# Add records from a CSV file
import records new_data_2025.csv --batch-size 10000 --progress

# Add with date formatting
import records new_data_2025.csv --date-column Time --date-format "M/D/YYYY" --batch-size 10000

# Add with validation
import records new_data_2025.csv --validate --batch-size 10000 --log-file import_log.txt
```

#### Complete Year Replacement Example
To replace all 2025 data in one operation:

```bash
# Step 1: Backup current data
backup create backup_before_2025_update.accdb

# Step 2: Delete 2025 data
delete records --where "YEAR(Time) = 2025" --batch-size 10000 --progress

# Step 3: Import new 2025 data
import records new_2025_data.csv --date-column Time --date-format "M/D/YYYY" --batch-size 10000 --progress

# Step 4: Verify the operation
validate records --where "YEAR(Time) = 2025" --count
```

#### Performance Tips for Large Operations
1. Use appropriate batch sizes (10,000 is recommended)
2. Enable progress tracking with `--progress`
3. Create backups before large operations
4. Use `--log-file` to track the operation
5. Consider running during off-hours
6. Close other applications to free up memory
7. Use `--validate` to ensure data integrity

#### Monitoring Large Operations
```bash
# Check operation status
status operations

# View operation logs
log show --operation import --limit 100

# Monitor system resources
status system
``` 