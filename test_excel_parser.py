import os
import sys
from unittest.mock import patch

# Test the ingest functionality with ChromaDB
def test_ingest():
    print("Testing ingest functionality with ChromaDB...")
    
    # Mock user input
    test_inputs = ['1', 'D:\\antigravity\\TIC\\EXCEL']
    
    with patch('builtins.input', side_effect=test_inputs):
        try:
            # Run the excel_parser.py script
            exec(open('excel_parser.py').read())
            print("✅ Ingest test completed successfully!")
        except Exception as e:
            print(f"❌ Error during ingest test: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_ingest()
