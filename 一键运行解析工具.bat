@echo off
chcp 65001 >nul
echo 正在启动智能 Excel Mappings 处理中心...
echo ==================================================
cd /d "%~dp0"
python excel_parser.py
echo.
pause
