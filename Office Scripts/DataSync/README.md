# DataSync - Automated Excel to Access Data Import Tool

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

DataSync is a powerful Python-based tool for automated data synchronization between Excel files and Microsoft Access databases. It features intelligent file discovery, optimized import methods, and comprehensive data management capabilities.

## ✨ Key Features

- **🔍 Automatic File Discovery** - Automatically finds and processes Excel files from a designated directory
- **⚡ Optimized Performance** - Uses one-by-one processing method (15% faster than batch processing)
- **📁 Smart File Management** - Automatically moves processed files to archive directory
- **🛡️ Data Validation** - Built-in validation and error handling
- **📊 Progress Tracking** - Real-time progress updates and detailed reporting
- **🔧 Flexible Configuration** - Multiple database connection options
- **💻 CLI & Interactive** - Both command-line and interactive menu interfaces

## 🚀 Quick Start

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/DataSync.git
cd DataSync
```

2. Set up virtual environment:
```bash
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### Basic Usage

1. **Place Excel files** in the `data/` directory
2. **Run auto-import**:
```bash
python src\datasync\cli.py auto-import --database "C:\path\to\your\database.accdb"
```
3. **Verify** - processed files are automatically moved to `data/loaded/`

## 📋 Available Commands

### Auto-Import System (Recommended)
```bash
# Check system status
python src\datasync\cli.py status

# Auto-discover and import all new files
python src\datasync\cli.py auto-import --database "C:\path\to\database.accdb"

# Force import specific file
python src\datasync\cli.py force-import "data\file.xlsx" --database "C:\path\to\database.accdb"

# Interactive menu
python src\datasync\cli.py menu
```

### Database Operations
```bash
# View database tables
python src\datasync\cli.py validate "C:\path\to\database.accdb" --preview

# Diagnose system setup
python src\datasync\cli.py diagnose
```

## 🗂️ Directory Structure

```
DataSync/
├── data/                 # Place new Excel files here
│   ├── loaded/          # Processed files are moved here
│   └── .gitkeep
├── src/
│   └── datasync/
│       ├── cli.py       # Command-line interface
│       ├── services/    # Import services
│       ├── utils/       # Utilities and file discovery
│       └── processing/  # Excel processing
├── config/              # Configuration files
└── logs/               # Application logs
```

## ⚙️ Configuration

### Method 1: Command Line Parameter (Recommended)
```bash
python src\datasync\cli.py auto-import --database "C:\path\to\production.accdb"
```

### Method 2: Environment Variable
```bash
set DATASYNC_DATABASE=C:\path\to\production.accdb
python src\datasync\cli.py auto-import
```

### Method 3: Configuration Helper
```bash
python setup_production.py
```

## 📊 Performance Optimization

DataSync uses performance-tested methods:

- **One-by-one processing**: 15% faster than batch processing (283.76s vs 333.29s average)
- **Batch database operations**: Configurable batch sizes for optimal throughput
- **Memory management**: Efficient processing of large datasets
- **Error recovery**: Robust error handling with detailed reporting

## 🔒 Security & Best Practices

- **Explicit database paths** - No hardcoded production database paths
- **Automatic backups** - File archiving and audit trails
- **Validation** - Data type checking and conversion
- **Error handling** - Comprehensive error reporting and recovery

## 📖 Documentation

- [User Instructions](User_Instructions.md) - Comprehensive usage guide
- [API Documentation](docs/) - Technical documentation
- [Configuration Guide](config/) - Setup and configuration details

## 🧪 Testing

```bash
# Run tests
python -m pytest src/tests/

# Run performance tests
python test_import_cycle.py --mode C --iterations 10
```

## 🛠️ Development

### Project Structure
- `src/datasync/` - Main application code
- `src/tests/` - Test suites
- `config/` - Configuration files
- `docs/` - Documentation

### Key Components
- **CLI Interface** (`cli.py`) - Command-line interface
- **Import Service** (`services/data_import_service.py`) - Core import logic
- **File Discovery** (`utils/file_discovery.py`) - Automatic file detection
- **Excel Processor** (`processing/excel_processor.py`) - Excel file handling

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Adam Daves**
- Email: adam.daves@thaiunion.com
- Company: Thai Union

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📞 Support

For support and questions:
- Create an issue on GitHub
- Contact: adam.daves@thaiunion.com

---

**DataSync** - Streamlining data workflows with intelligent automation.
