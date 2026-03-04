@echo off
REM Windows executable launcher for view_db.py
echo Starting Vector DB Viewer...
echo ==================================================
cd /d "%~dp0"

python view_db.py
echo.
pause
