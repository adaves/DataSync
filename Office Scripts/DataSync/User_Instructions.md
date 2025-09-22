# DataSync - User Instructions

## Quick Start Guide

### 1. Installation

#### Option 1: Using the Executable (Recommended)
1. Copy the DataSync.exe file from OneDrive
2. Open Command Prompt (cmd.exe)
3. Navigate to the folder containing DataSync.exe
4. Run the application by typing: `DataSync.exe menu`

### 2. First-Time Setup

1. Open Command Prompt (cmd.exe)
2. Navigate to the folder containing DataSync
3. Run the application: `DataSync.exe menu`
4. When prompted, enter the path to your Microsoft Access database file (.accdb or .mdb)
5. Press Enter to establish the connection

### 3. Basic Operations Using the Interactive Menu

When you run `DataSync.exe menu`, you'll see the main menu with the following options:

#### 1. Database Operations
This option leads to a submenu with the following choices:

##### 1.1 View Tables
1. Select this option to see a list of all tables in your database
2. Choose a table by number to view its details
3. You will see the table structure (column names) and a preview of the data
4. You can then:
   - View more rows
   - Export the data to CSV
   - Return to the previous menu

##### 1.2 Add Records
1. Select this option to add new records to a table
2. Choose the target table from the list
3. You can add records:
   - Manually (entering values through prompts)
   - From an Excel file (providing the path to the Excel file)
4. When adding from Excel, you'll see real-time progress updates

##### 1.3 Delete Records
1. Select this option to delete records from a table
2. Choose the table to delete records from
3. Specify the criteria for deletion (e.g., by ID, date range, condition)
4. Confirm the deletion
5. You'll see real-time progress updates during deletion

#### 2. Exit
This option exits the application.

### 4. Command-Line Operations

If you prefer direct command-line operations instead of the interactive menu, you can use these commands:

#### NEW: Auto-Import System (Recommended)
```bash
# Check system status and see what files are ready for import
python src\datasync\cli.py status

# Automatically discover and import all new files from data directory (uses optimal one-by-one method)
python src\datasync\cli.py auto-import

# Auto-import with custom database path
python src\datasync\cli.py auto-import --database "C:\path\to\your\database.accdb"

# Auto-import with custom table and batch size
python src\datasync\cli.py auto-import --database "C:\path\to\database.accdb" --table "YourTableName" --batch-size 2000

# Force import a specific file
python src\datasync\cli.py force-import

# Force import with specific file and database
python src\datasync\cli.py force-import "data\your_file.xlsx" --database "C:\path\to\database.accdb"
```

#### Viewing Tables and Data
```bash
# View all tables in a database
python src\datasync\cli.py validate C:\path\to\database.accdb

# Preview data in all tables
python src\datasync\cli.py validate C:\path\to\database.accdb --preview

# View a specific table with data preview
python src\datasync\cli.py validate C:\path\to\database.accdb --table TableName --preview
```

#### Legacy Synchronizing Data
```bash
# Import data from Excel to Access
python src\datasync\cli.py sync C:\path\to\source.xlsx C:\path\to\destination.accdb

# Import with validation
python src\datasync\cli.py sync C:\path\to\source.xlsx C:\path\to\destination.accdb --validate

# Import with custom batch size (for large datasets)
python src\datasync\cli.py sync C:\path\to\source.xlsx C:\path\to\destination.accdb --batch-size 5000
```

### 5. Monthly Data Import Workflow (NEW)

DataSync now includes an optimized workflow for monthly data imports:

#### Setup (One-time)
1. **Prepare Directory Structure**: The system automatically creates:
   - `data/` - Place your new Excel files here
   - `data/loaded/` - Processed files are automatically moved here

2. **Configure Production Database**: 
   ```bash
   # Option 1: Specify database path each time
   python src\datasync\cli.py auto-import --database "C:\path\to\your\production\database.accdb"
   
   # Option 2: Update default database locations (see Configuration section below)
   ```

#### Monthly Workflow
1. **Download** your 2-3 monthly Excel files
2. **Place files** in the `data/` directory
3. **Check status**: `python src\datasync\cli.py status`
4. **Import**: `python src\datasync\cli.py auto-import --database "C:\path\to\production\database.accdb"`
5. **Verify**: Files are automatically moved to `data/loaded/` after successful import

#### Why This Method is Optimal
- Uses **one-by-one processing** (15% faster than batch processing based on performance testing)
- **Automatic file discovery** - no need to specify individual files
- **Automatic file management** - processed files are archived
- **Built-in validation** and error handling
- **Progress tracking** and detailed reporting

### 6. Production Database Configuration

#### Method 1: Command Line Parameter (Recommended)
Always specify your production database path:
```bash
python src\datasync\cli.py auto-import --database "C:\path\to\your\production\database.accdb"
```

#### Method 2: Update Default Database Locations
Edit the default locations in the CLI configuration by modifying the `DEFAULT_DB_LOCATIONS` in `src/datasync/cli.py`:
```python
DEFAULT_DB_LOCATIONS = [
    Path("C:\\path\\to\\your\\production\\database.accdb"),  # Add your production database
    Path.home() / "Documents" / "DataSync" / "database.accdb",
    Path.cwd() / "database.accdb",
]
```

#### Method 3: Environment Variable (Advanced)
Set an environment variable for your production database:
```bash
set DATASYNC_DATABASE=C:\path\to\your\production\database.accdb
python src\datasync\cli.py auto-import
```

### 7. Working with Large Datasets

DataSync includes special features for handling large datasets efficiently:

1. **Batch Processing**: When importing large amounts of data (e.g., 300,000+ rows), DataSync automatically uses batch processing to:
   - Prevent memory issues
   - Provide transaction safety (if an error occurs, completed batches are preserved)
   - Show progress updates during the operation

2. **Progress Tracking**: For long-running operations, you'll see real-time progress information:
   - Current batch number
   - Records processed so far
   - Percentage complete
   - Estimated time remaining (for very large operations)

3. **Error Handling**: If errors occur during batch operations:
   - Successfully processed batches are committed
   - The error is reported with details about which records failed
   - You can fix the source data and retry just the failed portion

### 6. Tips for Best Results

1. **Excel Data Preparation**:
   - Ensure column headers match the Access table field names (or be prepared to map them)
   - Check that data types are compatible (dates formatted as dates, numbers as numbers, etc.)
   - Remove any blank rows at the beginning or end of the Excel sheet

2. **Access Database Management**:
   - Compact and repair your Access database periodically for best performance
   - Back up your database before performing large delete or update operations
   - For very large databases (>1GB), consider using the batch operations exclusively

3. **Performance Considerations**:
   - Use appropriate batch sizes (default is 1000, increase for simple data structures)
   - Run DataSync on the same machine as the Access database when possible
   - Close other applications that might be using the database during large operations

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