@echo off
echo Starting DataSync...
cd /d "%~dp0"
call venv\Scripts\activate.bat
python -m datasync.cli menu
pause 