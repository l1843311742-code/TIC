@echo off
REM Windows executable launcher for excel_parser.py
echo Starting Excel Parser...
echo ==================================================
cd /d "%~dp0"

python excel_parser.py
echo.
pause
