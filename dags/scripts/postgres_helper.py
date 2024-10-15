import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import constants as c

# Create SQLAlchemy engine
engine = create_engine(
    f'postgresql+psycopg2://{c.postgres_user}:{c.postgres_password}@{c.postgres_host}:{c.postgres_port}/{c.postgres_dbname}'
)

def infer_sql_schema_from_parquet(df, table_name):
    """
    Infers the SQL schema from a DataFrame to create a table in PostgreSQL.
    """
    print(f"Inferring SQL schema for table: {table_name}")
    
    schema = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    
    # Mapping pandas data types to SQL data types
    type_mapping = {
        'object': 'TEXT',
        'int64': 'INTEGER',
        'float64': 'DECIMAL',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'timedelta[ns]': 'INTERVAL',
        'Int64': 'INTEGER',
        'Float64': 'DECIMAL',
        'datetime64[ns, UTC]': 'TIMESTAMP'
    }
    
    for col in df.columns:
        dtype = str(df[col].dtype)
        sql_type = type_mapping.get(dtype, 'TEXT')  # Default to TEXT if type is not mapped
        schema += f"    {col} {sql_type},\n"
    
    schema = schema.rstrip(',\n')  # Remove the last comma
    schema += "\n);"
    print(f"SQL schema inferred: \n{schema}")
    return schema

def run_sql(create_sql):
    """
    Executes an SQL command in the database.
    """
    print("Running SQL command to create table...")
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
    print("Table creation SQL command executed successfully.")

def upload_overwrite_table(df, table_name):
    """
    Overwrites a table in PostgreSQL with the data from a DataFrame.
    """
    print(f"Uploading data to PostgreSQL table: {table_name}")
    df.to_sql(f'{table_name}', engine, index=False, if_exists='replace')
    print(f"Data uploaded successfully to table: {table_name}")

def process_parquet_to_postgres(parquet_file, table_name):
    """
    Reads a parquet file, infers the schema, and loads the data into PostgreSQL.
    """
    print(f"Processing parquet file: {parquet_file}")
    
    # Read the parquet file
    df = pd.read_parquet(parquet_file)
    print(f"Parquet file read successfully. Number of rows: {len(df)}, Number of columns: {len(df.columns)}")
    
    # Print data types of each column in the DataFrame
    print(f"Data types of the columns in the DataFrame:\n{df.dtypes}\n")
    
    # Infer the SQL schema
    create_table_sql = infer_sql_schema_from_parquet(df, table_name)
    
    # Create the table in the database
    run_sql(create_table_sql)
    
    # Overwrite the table with the data from the DataFrame
    upload_overwrite_table(df, table_name)
    print(f"Processing of parquet file {parquet_file} completed.\n")

# Example usage with two parquet files from the GOLD layer
process_parquet_to_postgres('./data/GOLD/real_estate_agents_classified/real_estate_agents_classified.parquet', 'google_agents_data')
process_parquet_to_postgres('./data/GOLD/fmcsa_company_data/fmcsa_company_data.parquet', 'fmcsa_data')
