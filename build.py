import PyInstaller.__main__
import os
import shutil
from pathlib import Path

def build_executable():
    # Get the project root directory
    project_root = Path(__file__).parent
    
    # Define paths
    src_dir = project_root / "src"
    dist_dir = project_root / "dist"
    build_dir = project_root / "build"
    
    # Clean previous builds
    if dist_dir.exists():
        shutil.rmtree(dist_dir)
    if build_dir.exists():
        shutil.rmtree(build_dir)
    
    # Create the executable
    PyInstaller.__main__.run([
        'src/datasync/cli.py',  # Main script
        '--name=DataSync',
        '--onefile',  # Create a single executable
        '--windowed',  # Don't show console window
        '--icon=assets/icon.ico',  # Application icon
        '--add-data=config;config',  # Include config files
        '--add-data=.env;.',  # Include .env file
        '--hidden-import=pandas',
        '--hidden-import=numpy',
        '--hidden-import=pyodbc',
        '--hidden-import=openpyxl',
        '--hidden-import=python-dotenv',
        '--hidden-import=pyyaml',
        '--clean',  # Clean PyInstaller cache
        '--noconfirm',  # Replace output directory without asking
    ])
    
    # Create installer directory
    installer_dir = project_root / "installer"
    installer_dir.mkdir(exist_ok=True)
    
    # Copy the executable to installer directory
    executable = dist_dir / "DataSync.exe"
    if executable.exists():
        shutil.copy2(executable, installer_dir)
        print(f"Executable created successfully at: {installer_dir / 'DataSync.exe'}")
    else:
        print("Error: Executable not found in dist directory")

if __name__ == "__main__":
    build_executable() 