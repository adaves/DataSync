# DataSync

A robust utility for synchronizing data between Microsoft Excel files and Microsoft Access databases.

## Features

- View and select tables from Access databases
- Filter and read data by date (full date or year)
- Delete data with automatic backup to temporary tables
- Upload new data from Excel files
- Memory-efficient handling of large datasets (up to 3M rows)
- Progress tracking for long operations
- Automatic cleanup of temporary backup tables

## Requirements

- Python 3.13.3
- Windows OS
- Microsoft Office 2503 (Access and Excel)
- Microsoft Access Driver (*.mdb, *.accdb)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/datasync.git
   cd datasync
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the application:
```bash
python -m src.datasync
```

The application provides a command-line interface with numbered options for:
1. Viewing Access database tables
2. Selecting and filtering table data
3. Deleting data (with automatic backup)
4. Uploading new data from Excel

## Development

### Project Structure
```
datasync/
├── src/
│   ├── app/
│   │   ├── database/    # Database operations
│   │   ├── utils/       # Utility functions
│   │   └── cli.py       # Command-line interface
│   └── datasync.py      # Main entry point
├── tests/               # Test files
├── docs/               # Documentation
├── scripts/            # Utility scripts
└── config/            # Configuration files
```

### Testing

Run tests:
```bash
python -m pytest
```

## License

[Insert License Information]
