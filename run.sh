#!/bin/bash
# Mac/Linux execution script for excel_parser.py
echo "Starting Excel Parser..."
echo "=================================================="

# Move to the directory where this script is located
cd "$(dirname "$0")"

python3 excel_parser.py
