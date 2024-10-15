import os
import pandas as pd
from .upsert import upsert_to_postgres

# Function to process parquet file and load it into PostgreSQL
def process_parquet_to_postgres(parquet_file, table_name, engine):
    print(f"Processing parquet file: {parquet_file}")
    
    # Read the parquet file
    df = pd.read_parquet(parquet_file)
    print(f"Parquet file read successfully. Number of rows: {len(df)}, Number of columns: {len(df.columns)}")
    
    # Print data types of each column in the DataFrame
    print(f"Data types of the columns in the DataFrame:\n{df.dtypes}\n")
    
    # Get modification time of the parquet file
    gold_file_mtime = os.path.getmtime(parquet_file)
    
    # Perform UPSERT into PostgreSQL
    upsert_to_postgres(df, table_name, gold_file_mtime, engine)
    print(f"Processing of parquet file {parquet_file} completed.\n")
