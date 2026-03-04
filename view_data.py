import lancedb
import pandas as pd

def view_data(db_path="./vector_store", table_name="mapping_collection"):
    try:
        # Connect to the database
        db = lancedb.connect(db_path)
        
        # Open the specific table
        table = db.open_table(table_name)
        
        # Convert the entire table to a pandas DataFrame
        df = table.to_pandas()
        
        # Configure pandas options for better console viewing
        pd.set_option('display.max_columns', None)  # Show all columns
        pd.set_option('display.width', 2000)        # Prevent wrapping
        
        # Drop the dummy vector column for cleaner display, as it's just meant for embeddings
        output_df = df.drop(columns=['vector'])
        
        print("\n=== LanceDB 数据库内容 ===")
        print(f"总计包含 {len(output_df)} 条数据。\n")
        print(output_df)
        print("\n==========================")
        
    except Exception as e:
        print(f"读取数据时出错：{e}")

if __name__ == "__main__":
    view_data()
