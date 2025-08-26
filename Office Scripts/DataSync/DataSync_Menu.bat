@echo off
cd /d "%~dp0"
echo Starting DataSync Menu...
echo.

REM Try the fixed Python source first (recommended)
if exist "src\datasync\cli.py" (
    echo Using Python source (with fixes)...
    python src\datasync\cli.py menu
    goto end
)

REM Fallback to executables
if exist "installer\DataSync.exe" (
    echo Using compiled executable...
    installer\DataSync.exe menu
) else if exist "DataSync.exe" (
    echo Using compiled executable...
    DataSync.exe menu
) else (
    echo DataSync not found!
    echo Please make sure Python is installed or the executable is built.
)

:end
pause
