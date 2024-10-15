import pandas as pd
from datetime import datetime
from sqlalchemy import text
from .db_operations import table_exists, run_sql, display_table_head
from .schema_inference import infer_sql_schema_from_parquet

# Function to check if the data in the GOLD layer is more recent than the data in the table
def is_gold_data_newer(table_name, gold_file_mtime, engine):
    query = f"SELECT MAX(updated_at) FROM {table_name};"
    with engine.connect() as conn:
        result = conn.execute(text(query)).scalar()
    
    if result is None:
        return True  # No data in the table
    else:
        return gold_file_mtime > result.timestamp()  # Compare GOLD file mtime with the latest 'updated_at' in the table

# Function to perform UPSERT/MERGE based on 'updated_at'
def upsert_to_postgres(df, table_name, gold_file_mtime, engine):
    print(f"Checking if table {table_name} exists...")
    if not table_exists(table_name):
        print(f"Table {table_name} does not exist. Creating the table.")
        create_table_sql = infer_sql_schema_from_parquet(df, table_name)
        run_sql(create_table_sql)

        # Display the first 5 rows of the updated table
        display_table_head(table_name)
    
    print(f"Checking if GOLD data is newer than the current data in {table_name}...")
    if is_gold_data_newer(table_name, gold_file_mtime, engine):
        print(f"GOLD data is newer. Performing UPSERT.")
        df['updated_at'] = datetime.now()
        df.to_sql(table_name, engine, index=False, if_exists='replace')  # Overwrite the table
        print(f"Data UPSERT completed for table {table_name}.")

        # Display the first 5 rows of the updated table
        display_table_head(table_name)
    else:
        print(f"No update needed. GOLD data is older or equal to the data in {table_name}.")
