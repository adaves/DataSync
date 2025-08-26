import os
import shutil
import subprocess
import sys
from pathlib import Path

def build_executable():
    # Install required dependencies
    required_packages = ['pyinstaller', 'click', 'pandas', 'pyodbc', 'openpyxl', 'python-dotenv', 'pyyaml']
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))  # Handle package name differences
        except ImportError:
            print(f"Installing {package}...")
            subprocess.run([sys.executable, '-m', 'pip', 'install', package], check=True)
    
    # Get the project root directory
    project_root = Path(__file__).parent
    
    # Define paths
    src_dir = project_root / "src"
    dist_dir = project_root / "dist"
    build_dir = project_root / "build"
    config_dir = project_root / "config"
    cli_script = src_dir / "datasync" / "cli.py"
    
    # Check if required files exist
    if not cli_script.exists():
        print(f"Error: CLI script not found at {cli_script}")
        return
    if not config_dir.exists():
        print(f"Error: Config directory not found at {config_dir}")
        return
    
    # Clean previous builds
    try:
        if dist_dir.exists():
            shutil.rmtree(dist_dir)
    except PermissionError:
        print(f"Warning: Could not remove {dist_dir} - directory may be in use")
    
    try:
        if build_dir.exists():
            shutil.rmtree(build_dir)
    except PermissionError:
        print(f"Warning: Could not remove {build_dir} - directory may be in use")
    
    # Create the executable
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        str(cli_script),  # Main script
        '--name=DataSync',
        '--onefile',  # Create a single executable
        '--console',  # Show console window for debugging
        f'--add-data={config_dir};config',  # Include config files
        '--hidden-import=click',
        '--hidden-import=pandas',
        '--hidden-import=numpy',
        '--hidden-import=pyodbc',
        '--hidden-import=openpyxl',
        '--hidden-import=python-dotenv',
        '--hidden-import=pyyaml',
        '--hidden-import=src.database.operations',
        '--hidden-import=datasync.utils.logging',
        '--clean',  # Clean PyInstaller cache
        '--noconfirm',  # Replace output directory without asking
    ]
    
    subprocess.run(cmd, check=True)
    
    # Create installer directory
    installer_dir = project_root / "installer"
    installer_dir.mkdir(exist_ok=True)
    
    # Find the executable (PyInstaller output location varies)
    possible_locations = [
        installer_dir / "dist" / "DataSync.exe",  # Current build location
        dist_dir / "dist" / "DataSync.exe",       # Previous location
        dist_dir / "DataSync.exe",                # Direct location
        installer_dir / "DataSync.exe"            # Already in installer dir
    ]
    
    executable_found = None
    for location in possible_locations:
        if location.exists():
            executable_found = location
            break
    
    if executable_found:
        if executable_found.parent != installer_dir:
            shutil.copy2(executable_found, installer_dir)
        print(f"✅ Executable created successfully at: {installer_dir / 'DataSync.exe'}")
        print(f"📁 File size: {(installer_dir / 'DataSync.exe').stat().st_size / (1024*1024):.1f} MB")
    else:
        print("❌ Error: Executable not found in any expected locations")
        print("📍 Searched in:")
        for loc in possible_locations:
            print(f"   - {loc}")

if __name__ == "__main__":
    build_executable() 