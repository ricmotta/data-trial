from sqlalchemy import create_engine, text
import scripts.constants as c
import pandas as pd

# Create SQLAlchemy engine
engine = create_engine(
    f'postgresql+psycopg2://{c.postgres_user}:{c.postgres_password}@{c.postgres_host}:{c.postgres_port}/{c.postgres_dbname}'
)

# Function to check if a table exists in the PostgreSQL database
def table_exists(table_name):
    query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    );
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).scalar()
    return result

# Function to execute an SQL command
def run_sql(create_sql):
    print("Running SQL command to create or modify the table...")
    with engine.connect() as conn:
        conn.execute(text(create_sql))
    print("SQL command executed successfully.")

# Function to display the first 5 rows of a table
def display_table_head(table_name):
    """
    Display the first 5 rows of the given table in the Postgres database.
    """
    print(f"Fetching first 5 rows from table: {table_name}")
    query = f"SELECT * FROM {table_name} LIMIT 5;"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            print(df)
    except Exception as e:
        print(f"Error fetching data from table {table_name}: {e}")
